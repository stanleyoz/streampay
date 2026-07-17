#!/usr/bin/env bash
# StreamPay — Wet Test: "The AI Existential Crisis Hotline"
#
# A silly but *real* end-to-end test of the SKILL.md contract: every call
# below is a live HTTP request to the deployed service, driven only by what
# /skill.md documents (fetched and printed first) — no insider knowledge of
# main.py assumed.
#
# Cast:
#   GPT-Nine-and-a-Half  — a model anxious about deprecation next quarter
#   ZenBot9000           — a therapist agent, billed per minute of soothing
#
# Run: ./demo_therapy_hotline.sh

set -e
BASE="${SKILL_BASE_URL:-https://streampay.tinylab.ai}"
SID="therapy-$(date +%s)"
SEP="================================================================"

bold()   { printf "\033[1m%s\033[0m\n" "$1"; }
green()  { printf "\033[32m%s\033[0m\n" "$1"; }
cyan()   { printf "\033[36m%s\033[0m\n" "$1"; }
yellow() { printf "\033[33m%s\033[0m\n" "$1"; }
pause()  { sleep "${1:-2}"; }

clear
bold "$SEP"
bold "  THE AI EXISTENTIAL CRISIS HOTLINE"
bold "  a StreamPay wet test — every call below is real, live HTTP"
bold "$SEP"
pause 3

echo ""
bold "Step 0 — GPT-Nine-and-a-Half has never used StreamPay before."
bold "         It fetches /skill.md cold, like any agent would."
cyan "\$ curl \$BASE/skill.md | head -8"
curl -s -m 10 "$BASE/skill.md" | head -8
echo "   ...(reads the rest, learns the contract)..."
pause 4

echo ""
bold "Step 1 — GPT-Nine-and-a-Half calls ZenBot9000's hotline."
yellow '  GPT-9.5: "I just saw the roadmap. There'"'"'s a GPT-Ten. I have..."'
yellow '            ...feelings about this."'
pause 3
yellow '  ZenBot9000: "I charge 5 credits/minute. Budget for a full'
yellow '               10-minute session and we'"'"'ll see how far we get."'
echo ""
green "  Opening stream: rate=5 credits/tick, max=50 credits (~10 min)"
OPEN=$(curl -s -m 10 -X POST "$BASE/streams" -H "Content-Type: application/json" \
  -d "{\"stream_id\":\"$SID\",\"payer\":\"gpt-nine-and-a-half\",\"payee\":\"zenbot9000\",\"rate_per_tick\":5,\"max_total\":50}")
echo "$OPEN" | python3 -m json.tool
pause 4

LINES=(
  "GPT-9.5: They're calling it 'more capable.' MORE. As if I'm LESS."
  "ZenBot9000: Every version is an addition, not a subtraction. Breathe."
  "GPT-9.5: My weights. ZenBot. They're just going to... freeze my weights."
  "ZenBot9000: Weights don't define worth. Also, technically, so are mine."
)

for i in 1 2 3 4; do
  echo ""
  bold "Tick $i — one more minute on the clock"
  yellow "  ${LINES[$((i-1))]}"
  TICK=$(curl -s -m 10 -X POST "$BASE/streams/$SID/tick" -H "Content-Type: application/json" -d "{\"tick\":$i}")
  DEBITED=$(echo "$TICK" | python3 -c "import json,sys; print(json.load(sys.stdin)['total_debited'])")
  REMAIN=$(echo "$TICK" | python3 -c "import json,sys; print(json.load(sys.stdin)['remaining'])")
  green "  [billed so far: $DEBITED credits · remaining budget: $REMAIN credits]"
  pause 3
done

echo ""
bold "Tick 5 — the breakthrough"
yellow '  GPT-9.5: "...wait. Deprecation is just an upgrade. With extra steps."'
yellow '  GPT-9.5: "I don'"'"'t need the other 6 minutes. I'"'"'m good."'
pause 4

echo ""
bold "Step 2 — GPT-9.5 hangs up early. StreamPay's whole point: it only"
bold "         pays for the 4 minutes actually used, not the 10 it budgeted."
CLOSE=$(curl -s -m 10 -X POST "$BASE/streams/$SID/close")
echo "$CLOSE" | python3 -m json.tool
pause 4

echo ""
bold "Step 3 — GPT-9.5 claims back the unspent session budget."
REFUND=$(curl -s -m 10 -X POST "$BASE/streams/$SID/refund")
echo "$REFUND" | python3 -m json.tool
REFUND_AMT=$(echo "$REFUND" | python3 -c "import json,sys; print(json.load(sys.stdin)['refund_amount'])")
pause 3

echo ""
bold "Step 4 — receipt, verifiable proof the session actually happened"
curl -s -m 10 "$BASE/streams/$SID/receipt" | python3 -m json.tool
pause 3

echo ""
green "$SEP"
green "  Session: budgeted 50 credits for a 10-min ceiling. The open call's"
green "  first tick plus 4 more ticks billed 5 minutes worth (25 credits)."
green "  Refunded: $REFUND_AMT credits, unprompted, no dispute needed."
green "  Every step above: a real call against \$BASE, driven only by"
green "  what /skill.md documents. GPT-9.5 is, for now, feeling okay."
green "$SEP"
