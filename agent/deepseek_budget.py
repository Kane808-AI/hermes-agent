"""Read-only local status: python -m agent.deepseek_budget. No inference or notifications."""
from __future__ import annotations

import argparse
import json
import sqlite3

from agent.deepseek_budget_policy import BudgetDenied, load_policy, PRICING_SOURCE, PRICING_REVIEWED, RATES_EXPIRE
from agent.deepseek_budget_ledger import Ledger


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--initialize', action='store_true', help='Create the initial-day hold only; never clear it or grant authority')
    parser.add_argument('--activate-transition-cents', type=int,
                        help='Record an already-authorized initial-day prospective allowance; never grants spending authority')
    parser.add_argument('--authorization-ref', help='Non-secret reference to the explicit approval for that allowance')
    parser.add_argument('--authorized-through-day', help='Last authorized Pacific date; initial day or following day only')
    args = parser.parse_args(argv)
    activating = args.activate_transition_cents is not None
    if bool(args.authorization_ref) != activating or bool(args.authorized_through_day) != activating:
        parser.error('transition activation requires a cents limit, authorization reference and last authorized Pacific date')
    try:
        policy = load_policy()
        if args.initialize and policy is not None:
            Ledger(policy.ledger_path, mode=policy.mode).initialize()
        if args.activate_transition_cents is not None:
            if policy is None:
                raise BudgetDenied('DeepSeek budget: policy must be enabled before transition activation')
            Ledger(policy.ledger_path, mode=policy.mode).activate_transition(args.activate_transition_cents, args.authorization_ref,
                                                          args.authorized_through_day)
        result = {"enabled": False, "state": "not activated"} if policy is None else {
            "enabled": True, **Ledger(policy.ledger_path, mode=policy.mode).status(),
            "mapped_accounts": sorted(set(policy.accounts.values())),
            "pricing_reviewed": PRICING_REVIEWED, "pricing_source": PRICING_SOURCE,
            "rates_expire_utc": RATES_EXPIRE.isoformat(),
            "coverage": "patched Hermes HTTP clients only; excludes external clients and older running code",
            "authority": "protection only; project external-spend holds still apply",
        }
    except (BudgetDenied, OSError, ValueError, KeyError, sqlite3.Error) as exc:
        result = {"state": "blocked", "error": str(exc)}
    print(json.dumps(result, sort_keys=True, indent=2))


if __name__ == "__main__":
    main()
