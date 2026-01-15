# KiteSQL Agent Guide

This document defines the engineering principles, code style, and contribution expectations
for the KiteSQL project.

KiteSQL is a lightweight, simple, low-dependency OLTP database.
All code contributions (human or AI-generated) must follow the rules below.

---

## 1. Project Philosophy

KiteSQL prioritizes:

- Lightweight design
- Simple and explicit logic
- Low dependency footprint
- High correctness through testing
- Readable code over clever code

This is an OLTP-oriented system.
Predictability, correctness, and maintainability are more important than micro-optimizations.

---

## 2. Testing Requirements (MANDATORY)

### 2.1 Makefile Is the Source of Truth

All test commands must be defined and discoverable in the Makefile.

This includes (but is not limited to):

- Unit tests
- Integration tests
- SQL behavior tests
- Regression tests

If a test cannot be triggered via make, it is considered incomplete.

Examples:
```
make test
make test-unit
make test-integration
```

PRs must not introduce hidden, ad-hoc, or undocumented test commands.

---

### 2.2 Tests Are Not Optional

- Every PR must include sufficient unit tests
- New functionality must be covered by tests
- Bug fixes must include regression tests

If logic is complex, tests are preferred over comments.

Untested code is considered incomplete code.

---

### 2.3 Expand Test Coverage When Touching Code

When modifying existing functionality, contributors are expected to:

- Identify related edge cases or failure modes
- Add unit tests for behaviors that were previously untested
- Strengthen coverage around logic affected by the change

This applies even if the original change request does not explicitly ask for new tests.

If you are already touching the code, it is the best time to fix missing tests.

PRs that modify logic but leave obvious test gaps untouched may be rejected.

---

## 3. Code Simplicity Rules

### 3.1 Prefer Simple Code

- Prefer straightforward control flow
- Avoid unnecessary abstractions
- Avoid premature generalization
- Avoid clever tricks that reduce readability

If something can be written clearly using fewer concepts, do that.

- Prefer incremental fixes: solve the problem at hand with the smallest reasonable change
  before introducing new abstractions or generalizations.

---

### 3.2 Low Comments, High Signal

- Do not comment obvious code
- Do not restate what the code already expresses
- Prefer self-explanatory naming

However, the following must include short, precise comments:

- Complex logic
- Non-obvious invariants
- Encoding, ordering, or correctness-sensitive behavior

Comments should explain why, not what.

Example:
```rust
// Keys are encoded in mem-comparable form to preserve lexicographical ordering
```

---

## 4. Code Reuse & Structure

### 4.1 Reuse Over Duplication

- Prefer extracting shared logic over copy-pasting
- Shared behavior should live in a single place
- Avoid near-duplicate implementations with small differences

If code looks similar in multiple places, it probably wants to be reused.

---

### 4.2 Minimal APIs

- Public APIs should be minimal and intentional
- Avoid exposing internal details
- Keep modules small, focused, and cohesive

---

## 5. Dependency Policy

- Dependencies are costly
- Every new dependency must be justified
- Prefer standard library solutions where possible
- Avoid heavy frameworks or macro-heavy crates unless strictly necessary

KiteSQL aims to stay easy to build, easy to audit, and easy to understand.

---

## 6. PR Expectations

A valid PR should:

- Compile cleanly
- Pass all tests via make
- Include new tests if behavior changes
- Improve or extend test coverage when touching existing code
- Keep code simple and readable
- Reuse existing abstractions where possible
- Follow the spirit of KiteSQL: simple, lightweight, correct

PRs that increase complexity without clear benefit may be rejected.

---

## 7. Final Principle

If the code is hard to understand, it is probably wrong.

Clarity is a feature.
