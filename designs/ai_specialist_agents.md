# AI Specialist Agents

## Purpose

IFX_ODIN uses a small set of project-scoped AI specialists to bring deliberate
perspectives into feature work. The primary agent still owns the plan,
implementation, validation, and final answer. Specialists inspect and advise;
they do not make competing edits.

## Specialists

| Specialist | When it participates | What it contributes |
| --- | --- | --- |
| UX designer | Before user-interface or user-workflow changes | User journey, interaction model, states, accessibility, and acceptance criteria |
| Software architect | Before material changes to ownership, public contracts, persistence, dependency direction, or architectural patterns | Design shape, boundaries, reuse decisions, and tradeoffs |
| Code reviewer | After feature implementation and tests | Concrete findings about correctness, security, concurrency, regressions, maintainability, and test gaps |

The software architect is selective: routine work that follows an established
design does not require consultation. It also treats composition roots as
wiring boundaries, not
as homes for parsing, credential translation, domain adaptation, or migration
fallback policy. Any temporary compatibility layer should identify its current
consumer, owner, deletion gate, and target release; when a dependency can be
made mandatory, a decisive cutover is preferred over open-ended dual routing.

The architect and code reviewer remain separate to preserve an independent
post-implementation check. The reviewer evaluates the code that was actually
built and should not be anchored to, or repeat, the architect's proposed shape.

The agent definitions live in `.codex/agents/`. They are configured and
instructed to work read-only so that advice remains separate from implementation
and concurrent specialists do not normally modify the same files.

## Collaboration Flow

1. The primary agent determines which pre-implementation perspectives apply.
2. UX and architecture consultations may run together because neither edits
   files.
3. An implementation-stage UX review renders the affected pages with
   representative data at desktop and mobile widths. If rendering is blocked,
   the review is explicitly incomplete.
4. The primary agent summarizes any material choice for the user. Specialists
   provide a recommended default so routine work does not stall on minor
   questions.
5. After agreement where needed, the primary agent implements and validates the
   feature.
6. The code reviewer examines the completed change. The primary agent addresses
   substantive findings and reports the outcome.

This is selective rather than ceremonial. Documentation-only edits and trivial
mechanical changes do not need a specialist pass. A user can also request or
decline a specialist explicitly for any task.

## Evolving the Agents

These files are team-owned working agreements. Adjust their preferences when a
review repeatedly produces low-value feedback, a missed concern recurs, or the
team develops a clearer design principle. Development feedback from the user is
an important source for those improvements: evaluate whether each comment
reveals a durable specialist preference or workflow. Put durable lessons into
the relevant agent instructions, and keep feature-specific decisions in that
feature's design so the specialist does not become narrowly overfit.

Add another specialist only when it has a distinct decision lens and a
repeatable workflow; avoid creating roles that merely restate the primary
agent's job.
