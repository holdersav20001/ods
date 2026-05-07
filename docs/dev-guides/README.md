# Developer guides

Three drafts of the same content for review — pick the format you want
canonical, or merge a hybrid. All three target the same workflow: how a
developer should populate `run_log`, `run_stage_log`, `lineage_edge`, and
`reconciliation_log` correctly, using the **event API** as the worked
example (smallest end-to-end ingestion in the repo).

| Style | File | When it earns its keep | Length |
|---|---|---|---|
| **A. Annotated walkthrough** | [`control-plane-A-annotated.md`](control-plane-A-annotated.md) | First-pass onboarding read. Builds the mental model by tracing real source line by line. | Long, narrative. |
| **B. Sequence diagram + companion** | [`control-plane-B-sequence.drawio`](control-plane-B-sequence.drawio) + [`control-plane-B-sequence.md`](control-plane-B-sequence.md) | Whiteboard / architecture-review reading. Diagram first, brief commentary. | Visual + 1-page text. |
| **C. Cookbook** | [`control-plane-C-cookbook.md`](control-plane-C-cookbook.md) | Day-to-day reference while coding. Recipes, table catalogue, common-mistakes table, decision rules. | Scannable, no narrative. |

## Suggested review questions

For each style, ask:

1. **"Could a brand-new dev write a correct integration tomorrow using only this doc?"**
   * A: most likely yes (slow read).
   * B: probably needs A or C alongside.
   * C: yes if they already know the architecture; no if pure cold-start.
2. **"Will this doc still be readable in 12 months when stages change?"**
   * A: medium — line-numbers drift but function names stable.
   * B: high — diagram describes the shape; less coupled to code.
   * C: high — table-shaped, easy to maintain.
3. **"Would I actually open this while coding?"**
   * A: probably no after first read.
   * B: yes for design discussions.
   * C: yes — keep tab open.

## Recommendation

Most teams keep **A** (onboarding) + **C** (daily) and treat **B** as a
poster on the wall. But this depends on your team's reading habits — pick
the one that matches how your team actually works.

If you want a hybrid: **C** as the canonical doc, with the **B** diagram
embedded near the top as the visual orientation, and **A** demoted to a
"long-form reference" appendix. Tell me which way to go and I'll
consolidate.
