import os
import json
import csv
import re
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATUSES_DIR = os.path.join(BASE_DIR, "statuses", "success")
RESPONSES_DIR = os.path.join(BASE_DIR, "responses", "success")
OUTPUT_CSV = os.path.join(BASE_DIR, "event_log.csv")

TIMESTAMP_RE = re.compile(r"^(\d{14})")  # YYYYMMDDHHmmss no início do nome do arquivo


def parse_timestamp_from_filename(filename: str) -> datetime | None:
    basename = os.path.basename(filename)
    m = TIMESTAMP_RE.match(basename)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%d%H%M%S")
    return None


def parse_file(filepath: str, source: str) -> list[dict]:
    events = []
    ts = parse_timestamp_from_filename(filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read().strip()

    # cada linha é um JSON independente
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        message_sid = data.get("MessageSid") or data.get("SmsSid")
        if not message_sid:
            continue

        if source == "status":
            activity = data.get("MessageStatus") or data.get("SmsStatus", "")
        else:
            # responses são SMS recebidos (reply do destinatário)
            activity = "received"

        query_params = data.get("queryParameters", {})
        campaign = query_params.get("campaign_name", "")

        events.append(
            {
                "case_id": message_sid,
                "activity": activity,
                "timestamp": ts.isoformat() if ts else "",
                "campaign_name": campaign,
                "from": data.get("From", ""),
                "to": data.get("To", ""),
            }
        )

    return events


def collect_events(directory: str, source: str) -> list[dict]:
    events = []
    for root, _, files in os.walk(directory):
        for fname in files:
            filepath = os.path.join(root, fname)
            try:
                events.extend(parse_file(filepath, source))
            except Exception as e:
                print(f"  [AVISO] Erro ao processar {filepath}: {e}")
    return events


def main():
    print("Coletando eventos de status...")
    events = collect_events(STATUSES_DIR, "status")
    print(f"  {len(events)} eventos coletados de statuses/")

    print("Coletando eventos de resposta (received)...")
    response_events = collect_events(RESPONSES_DIR, "response")
    print(f"  {len(response_events)} eventos coletados de responses/")

    events.extend(response_events)

    activity_order = {
        "sent": 0,
        "delivered": 1,
        "undelivered": 2,
        "failed": 3,
        "received": 4,
    }

    # Ordenar por timestamp, depois por case e por ordem de atividade esperada
    events.sort(
        key=lambda e: (
            e["timestamp"],
            e["case_id"],
            activity_order.get(e["activity"], 99),
        )
    )

    print(f"Total: {len(events)} eventos | {len({e['case_id'] for e in events})} casos únicos")

    fieldnames = ["case_id", "activity", "timestamp", "campaign_name", "from", "to"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(events)

    print(f"Event log salvo em: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
