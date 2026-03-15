# Lessons Learned

## Import paths must use `bot.` prefix

bot/ is a Python package. All internal imports and `patch()` targets must use `bot.xxx`, not `xxx`. Example:
- Source: `from bot.data.charts import ...` (not `from data.charts`)
- Test patch: `patch("bot.data.news.create_session")` (not `patch("data.news.create_session")`)

Short paths like `from data.xxx` work when `sys.path` includes `bot/`, but pytest from project root will raise `ModuleNotFoundError`.

## Test boundary values: watch strict inequality

When testing `< 2.0` threshold, using `change_1h=2.0` doesn't trigger the skip (equal is not less than). Use strictly-below-threshold values (e.g. 1.9) for boundary tests.

## Component dict keys must be unique

If a key is already taken (e.g. `summarizer` for OpenAI Summarizer), new components need distinct keys (e.g. `synthesizer_data`). The `_job()` helper uses `c[k]` to look up components — key must match exactly.

## Variable scope: initialize before conditional blocks

Variables assigned inside `if` blocks aren't available outside. Always initialize to `None` before the conditional when the value is needed later.

## SCP multi-file uploads can corrupt files

In zsh, chaining multiple `scp` commands with `&&` can cause file content to mix up. Use separate scp commands with absolute paths.

## DryRun tests are not integration tests

Unit tests with mock data passing does not mean the real API works. Always verify with `--dry-run` + inspect actual output before deploying:
- API response formats change (missing fields, renamed keys)
- AI output formatting issues only visible in real output
- Any change needs `--dry-run` + visual inspection before deployment

## Check Twitter reply_settings before engaging

Twitter API v2's `reply_settings` field (everyone / mentionedUsers / following) controls who can reply. Many large accounts restrict replies — attempting to reply without checking returns 403.

## MagicMock is not iterable

When `engager.get_mentions()` returns MagicMock, `for m in mentions` throws TypeError. Any method that iterates API responses needs `isinstance(result, list)` checks or try/except wrapping.

## SYSTEM_PROMPT rules are not runtime guarantees

Writing "every tweet must include $CASHTAG" in the system prompt doesn't guarantee compliance. LLMs don't follow rules 100%. Add programmatic validation + auto-injection after generation. Prompt = suggestion, code = guarantee.

## Multi-parameter function calls: verify data matches

`synthesizer.synthesize()` takes `exchange_data` and `derivatives_data` — easy to accidentally pass the same variable to both. When reviewing, check "does this variable actually contain what the function expects?"

## Deploy all modified files, not just current session

When multiple sessions modify the same project, deploy scripts miss files changed in other sessions. Before deploying: diff local vs remote file list, upload everything that changed.

## API permissions differ from browser permissions

Twitter API has stricter anti-automation limits than the web browser. Actions that work in-browser (reply, quote tweet) may return 403 via API for low-trust accounts. Reply 403 → QT fallback never works either (same restriction mechanism).

## Profile changes trigger Blue verification re-review

Changing avatar/name/bio triggers Twitter Blue re-verification. During review, blue checkmark disappears — replies get folded, search ranking drops. Change one field at a time, wait 24-48h between changes.

## Core growth channels need observability

Every stage of the engagement funnel (collect → filter → attempt → succeed) needs logging. Without it, you can only guess whether the problem is "0 candidates collected" or "50 collected but all filtered out."

## Twitter Polls lower interaction barrier

Polymarket yes/no questions are natural poll formats. `poster.post_poll()` and `poster.post_tweet()` are mutually exclusive (polls can't have media) — job logic needs branching.

## Twitter Communities API doesn't exist

tweepy `create_tweet` has no `community_id` parameter. Twitter API v2 has no community endpoints (join, browse feed, post). Community strategy is manual-only.
