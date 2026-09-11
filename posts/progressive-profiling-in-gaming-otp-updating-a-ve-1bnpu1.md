# Progressive Profiling in Gaming OTP — Updating a Verified User Without Recreating Identity

Phone verification in a game is an identity checkpoint, not a signup trigger. **Short answer: keep one immutable user ID, attach the verified phone as a claim, and make enrichment idempotent.** That prevents retries and bot traffic from splitting one player into several accounts.

I traced a duplicate-player incident after an OTP callback was retried. The handler treated the phone as a new account, so one verified person received two IDs and two abuse scores. A queue replay kept creating records while support moved inventory between them. The invariant in my runbook is blunt: verification may change claims, never the subject identifier.

## What should progressive profiling change after a user verifies a phone number?

Progressive profiling collects optional fields over time while preserving the account established by the first trusted factor. A game profile can gain a display name, age band, locale, or consent state after phone OTP. None of those values is a second identity key.

Keep identity and profile rows separate. The identity row owns a random, non-recycled `user_id`; the profile row stores mutable fields; a verification table stores a normalized phone hash and timestamp. A unique constraint on that hash prevents two accounts from claiming the same number. Updating a profile without recreating identity is the point of the design.

The write path is a transaction: validate the code, record an event ID, mark the existing subject verified, then apply profile changes. Recreating a user without checking that event ID makes a mobile retry look like a new registration.

## How can updating a verified user resist bots and duplicate delivery?

Assume two submissions race, a client retries after a timeout, and a queue delivers the same event twice. Those are normal conditions. Use a uniqueness constraint plus an idempotency key, and rate-limit attempts by phone, account, device, and network reputation. OWASP recommends throttling and monitoring authentication failures; do not log OTP values or full phone numbers.

```go
type VerifyRequest struct {
    UserID, PhoneHash, EventID, Code string
}

func VerifyPhone(ctx context.Context, db *sql.DB, r VerifyRequest) error {
    tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
    if err != nil { return err }
    defer tx.Rollback()

    var seen bool
    if err := tx.QueryRowContext(ctx,
        `SELECT EXISTS (SELECT 1 FROM auth_events WHERE event_id = $1)`, r.EventID).Scan(&seen); err != nil {
        return err
    }
    if seen { return tx.Commit() }
    if !checkOTP(ctx, r.UserID, r.Code) { return errors.New("invalid verification code") }

    _, err = tx.ExecContext(ctx, `
        INSERT INTO verified_phones (user_id, phone_hash, verified_at)
        VALUES ($1, $2, now())
        ON CONFLICT (phone_hash) DO UPDATE SET verified_at = now()
        WHERE verified_phones.user_id = EXCLUDED.user_id`, r.UserID, r.PhoneHash)
    if err != nil { return err }
    if _, err = tx.ExecContext(ctx,
        `INSERT INTO auth_events (event_id, user_id, kind) VALUES ($1, $2, 'phone_verified')`, r.EventID, r.UserID); err != nil {
        return err
    }
    return tx.Commit()
}
```

The conflict clause accepts the same owner and rejects an ownership change. Return a generic linking response so bots cannot discover whether a phone belongs to another player. Issue rewards from the recorded event, never from a page refresh.

## Which operational checks keep identity enrichment safe?

Track verified phones per user, users per phone hash, duplicate event rate, rejected link attempts, and rewards per verification event. Alert when `users_per_phone_hash > 1` or replayed events alter state. An audit record should include event ID, user ID, decision, and reason code.

Measure twice.

The useful dashboard is not a single “OTP success” number. I want a time series split by region, carrier, device reputation, and account age, because a bot campaign can keep completion high while changing who completes it. During a replay investigation, the decisive query joined `auth_events` to reward grants by event ID; it showed one verification producing three grants even though the login endpoint reported a normal success rate. That led us to move the reward side effect behind the same idempotent event boundary and to sample rejected link attempts for manual review. The lesson applies to every optional field: observe the state transition, the actor that caused it, and whether repeating the request leaves the state unchanged.

For recovery, do not pick the newest duplicate in application code. Freeze transfers, compare the audit log, and run a reviewed merge that preserves the oldest subject ID. The merge needs its own idempotency key and dry-run report.

The catch is that in-place claims are not suitable when policy requires separate personas sharing one family number. Use explicit household identity and isolated login subjects then. Stick with a new identity only when the product can prove the person is meant to become a different subject; convenience is not enough.

I’m not sure one global threshold fits every game. Your mileage may vary with regional SMS delivery, so measure completion and abuse outcomes by region before changing limits. The durable rule is stable: one verified person, one subject ID, many audited claims.

## References

- https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html
- https://www.rfc-editor.org/rfc/rfc4226
- https://www.rfc-editor.org/rfc/rfc6238
- https://www.postgresql.org/docs/current/ddl-constraints.html
