# Pilot: can an AI model help identify the locomotives?

Status: draft for the user's review, 2026-11-15. Nothing here is built or run yet.

## 1. Why

RAT volunteers check each photo's loco number and name on the Data Fixes review screen
(`picaloco_web` `/admin/fixes`). The extractor already proposes values from the photo's typed
description, but:

- 32,237 photos have **no number** in their description (`loco_number` confidence `none`), and
  120,408 have no name. Each one is a photo a volunteer must study from scratch.
- Some photos have no loco in them at all, and volunteers have to find those one by one.
- A description can be wrong. Today only a human looking at the photo catches that.

The question for the pilot: **on a small sample, is a vision model accurate enough that its
answers are worth showing to volunteers as suggestions?** The pilot only measures. It changes no
data and adds nothing to the review screen.

## 2. The hard limit: photo size

The review photos are 320px-wide thumbnails. The FileMaker originals are only about 376px wide
(measured 2026-11-15, spec `picaloco_web` §12.3). A number on a smokebox plate or cab side is often
a few pixels high. **Expect number reading to be weak; the pilot measures how weak.** High-resolution
scans from RAT would change this (already an open client question).

Tasks that don't need fine detail are more promising:

| Task | Likely at 320px? | Value if it works |
|---|---|---|
| Is there a loco in the photo? | Good | Clears "no loco" photos fast; could pre-select X |
| What kind of loco (steam/diesel/electric, wheel arrangement, class)? | Fair | Flags descriptions that don't match the photo |
| Read the number / name | Poor to fair | Fills the 32k photos with no described number |

## 3. What the pilot does

1. **Pick about 200 photos** from the next unreviewed records in the `loco_number` queue, mixed on
   purpose: about 80 with a high-confidence suggested number, 60 with no number in the description,
   and 60 others (medium/low). (My call: a mix shows where the model helps and where it doesn't.)
2. **Run the model on each photo, blind:** the photo only, not the description, so it reports what it
   can see and can't just repeat the text. One request per photo, structured JSON out (§4).
3. **Volunteers review the same 200 photos as normal**, without seeing the model's answers (no bias).
   Their saved answers are the "truth". The 36 photos already reviewed are added as a first check.
4. **Compare** and write a short report (§5).

The script only reads: photos from Storage, and `rat.data_fix_value` rows. Results go to a local
file (`exports/ai_pilot/`, gitignored). **It never writes to `rat.*`**, and never to the Data Fixes
tables (Golden Rule 7).

## 4. What the model returns per photo

```json
{
  "loco_present": "yes | no | unsure",
  "loco_count": 1,
  "numbers_read": [{"text": "6853", "confidence": "high | medium | low", "where": "cab side"}],
  "names_read": [{"text": "Morehampton Grange", "confidence": "low", "where": "splasher"}],
  "traction": "steam | diesel | electric | other | unsure",
  "wheel_arrangement": "4-6-0 or unsure",
  "class_guess": "GWR Grange class or unsure",
  "notes": "one short sentence"
}
```

The prompt tells the model to report only characters it can actually see, and to say "unsure"
rather than guess. A wrong confident read is the worst outcome, because a volunteer might accept it.

## 5. How it's judged

| Measure | Compared with | Bar to be useful (my call, for the user to change) |
|---|---|---|
| "No loco" detection | Volunteers' No loco answers | When the model says "no", it's right at least 95% of the time |
| Number read, high confidence | Volunteers' saved number | Exact match at least 90%; wrong reads under 3% |
| Numbers found where the description had none | Volunteers' saved number | Any reliable gain is worth showing |
| Class / wheel arrangement | Description and a manual spot check of 30 | A mismatch flag is right most of the time |

The report gives these figures, 10 example photos of each kind of success and failure, the real cost
per photo, and a recommendation: show it to volunteers, show only some of it, or wait for better scans.

## 6. Model and cost (my calls)

- **Model:** Claude Opus 5.5 (`claude-opus-5-5`), Anthropic's default current model, with low effort
  (a short visual task). The server-side refusal fallback is switched on.
- **Estimate, measured properly in the pilot:** a 320px photo is about 100 input tokens, plus about
  500 for the instructions, and about 400 output tokens. At $4 / $20 per million tokens that's
  roughly **1p per photo**, so **about £2 for 200 photos**. The Batch API halves that and suits this
  work, because nobody waits for the answers.
- Only if the pilot succeeds would a whole-archive run be priced. At the same rate, all 141k photos
  come to roughly £1,300, or half that by batch. Whether a cheaper model holds the same accuracy is a
  separate measurement, done only if the user asks for it.

## 7. Before anything runs (needs the user)

1. **RAT's agreement to send photos to Anthropic's API.** The photos are already public on the
   search site, but it's their archive.
2. **An Anthropic API account** for the script (`ant auth login` or `ANTHROPIC_API_KEY`). The key
   lives in `.env`, never in git.
3. **OK to spend about £2–3** on the pilot.
4. **Volunteers to review the 200 pilot photos**, which they'd do anyway; I give the list as a queue
   filter link.

## 8. Not in the pilot

- No change to the review screen. If the pilot works, showing suggestions there is a separate design
  (likely a "Model thinks:" line and a pre-ticked X that the volunteer confirms).
- No whole-archive run.
- A no-AI check worth doing separately: published numbering tables map each number to its class
  (for example 6853 belongs to the GWR Grange class), so a number that doesn't fit the class named
  in the description can be flagged by rules alone.
