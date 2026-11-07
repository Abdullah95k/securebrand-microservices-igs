# Recording what a logged-out visitor can search on Instagram

For the owner, from the igdisc session, 6 November 2026 (Iraq time). This is option (a), which you chose on 6 November: you record, and nothing runs on igs's exits or on the shared server.

**Time needed:** about 15 minutes.

**What it settles:** whether hashtag pages, the search box, keyword results, place pages, audio pages and suggested accounts work without a login today ([`igdisc-plan.md` §2](../master/igdisc-plan.md#2-what-a-logged-out-visitor-can-search-on-instagram-today)).

## Before you start

- Use **Chrome** (or another Chromium browser) in a **new Incognito window**, so nothing from your own Instagram login is present.
- **Don't log in at any point.** If a login box or a "sign up to see more" wall appears, that is the answer for that page: note it and go on. Don't close the box with tricks, and don't solve any challenge.
- Use your usual connection, not a VPN.
- Open DevTools (F12), go to **Network**, and tick **Preserve log** and **Disable cache**.

## For each step

1. In DevTools Network, clear the list (the 🚫 button).
2. Open the page, wait 5 seconds, then scroll down slowly three times.
3. Save it with the **download arrow (Export HAR)** in the Network panel. Chrome's default export is sanitized, which leaves out cookies and login headers. Name the file as in the table.
4. Write one line in the "What you saw" column: posts shown (roughly how many), and whether a login wall appeared and when (at once, or after scrolling).

| # | File name | Open this | What you saw |
|---|---|---|---|
| 1 | `01-hashtag-en.har` | `https://www.instagram.com/explore/tags/baghdad/` | |
| 2 | `02-hashtag-ar.har` | `https://www.instagram.com/explore/tags/بغداد/` | |
| 3 | `03-keyword.har` | `https://www.instagram.com/explore/search/keyword/?q=baghdad` | |
| 4 | `04-search-box.har` | `https://www.instagram.com/`, then click **Search** in the left menu if it shows, type `baghdad`, wait 3 seconds, press Enter | |
| 5 | `05-topsearch.har` | `https://www.instagram.com/web/search/topsearch/?query=baghdad` | |
| 6 | `06-place.har` | Open any public post of an account you already monitor that shows a **place name** under the account name, and click the place name | |
| 7 | `07-audio.har` | Open any public reel and click the **sound name** at the bottom | |
| 8 | `08-explore.har` | `https://www.instagram.com/explore/` | |
| 9 | `09-profile-suggested.har` | Any public profile you already monitor: look for "Suggested for you" or "Similar accounts" near the top | |

## Afterwards

- Put the 9 files and the filled table in the same Drive folder as your earlier recordings (C1), and share it **by link**. Please don't paste the files into a chat or into git.
- Note the date and the Iraq time you recorded them.

## What I do with them

- I write a findings note (`docs/igdisc/har-findings-<date>.md`) with request shapes, statuses, counts and times only: no usernames, names or post text.
- The files themselves stay out of git.
- The plan's §2 is updated from the findings. If something works logged out, the next step is the tiny production probe, and only with your approval.
