from __future__ import annotations

import csv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utilis.db import get_client_connection


OUTPUT_PATH = ROOT / "uploads" / "sysdiagrams.csv"


def main() -> int:
    conn = get_client_connection("insurance")
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM [dbo].[sysdiagrams]")
        columns = [str(column[0]) for column in cur.description or []]
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(columns)
            rows = 0
            while True:
                batch = cur.fetchmany(1000)
                if not batch:
                    break
                writer.writerows(batch)
                rows += len(batch)
    finally:
        conn.close()

    print(f"EXPORTED rows={rows} path={OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
