#!/usr/bin/env python3
"""
generate_realistic_bordereaux.py

Generate more realistic synthetic bordereaux data for three carriers:
    - CarrierAlpha_Phase1_Bordereaux.xlsx
    - CarrierBeta_Phase1_Bordereaux.xlsx
    - CarrierGamma_Phase1_Bordereaux.xlsx

These files are saved under: data/raw/

Columns:
    Effective Date
    Expiration Date
    Gross Premium
    Quota Share %
    Commission Rate
    Commission
    Ceded Commission
    Net Premium
    Product
    Premium State
    Principal
    Principal / Account Mailing Address
    Penal Amount
    Broker Name
    Broker State
    Obligee Name
    Obligee State
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


# -----------------------------
# Config
# -----------------------------
OUTPUT_DIR = "data/raw"

CARRIERS = [
    ("CarrierAlpha", 45),
    ("CarrierBeta", 40),
    ("CarrierGamma", 35),
]

# Some realistic products
PRODUCTS = [
    "Solar EPC Performance Bond",
    "Solar Decommissioning Bond",
    "Solar Interconnection Bond",
    "Community Solar Performance Bond",
    "Storage + Solar Combo Bond",
]

# States and some representative ZIPs
STATE_ZIP_MAP = {
    "PA": ["17815", "19103", "15222", "19610"],
    "MA": ["02115", "02139", "02215", "01810"],
    "GA": ["30309", "30308", "30303"],
    "WA": ["98101", "98109", "98004"],
    "CA": ["90027", "94105", "92101", "94704"],
    "TX": ["77002", "78701", "75201"],
    "CO": ["80202", "80903"],
}

BROKER_NAMES = [
    "Blue Horizon Brokerage",
    "Summit Risk Partners",
    "Evergreen Risk Advisors",
    "Atlantic Surety Group",
    "SolarSure Brokers",
]

OBLIGEE_NAMES = [
    "City of Bloomsburg",
    "Commonwealth Energy Authority",
    "County Infrastructure Board",
    "Metropolitan Transit Authority",
    "State Renewable Development Fund",
]

PRINCIPAL_NAMES = [
    "Sunrise Solar LLC",
    "GreenBeam Energy Inc.",
    "BrightFuture Renewables",
    "NovaGrid Power Solutions",
    "EcoArray Development Partners",
]

STATES = list(STATE_ZIP_MAP.keys())


# -----------------------------
# Helpers
# -----------------------------
def random_date(start_year=2023, end_year=2025):
    """Generate a random effective date and expiry about 1 year apart."""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta_days = (end - start).days
    eff = start + timedelta(days=random.randint(0, delta_days))
    exp = eff + timedelta(days=365)  # 1-year term
    return eff.date(), exp.date()


def generate_one_row():
    """Generate a single realistic project row."""
    # Dates
    eff, exp = random_date()

    # Geography
    state = random.choice(STATES)
    zip_code = random.choice(STATE_ZIP_MAP[state])

    # Premium logic
    gross_premium = round(random.uniform(2000, 75000), 2)  # total written premium
    quota_share_pct = round(random.uniform(0.2, 0.6), 2)   # 20%–60%
    commission_rate_pct = round(random.uniform(0.10, 0.25), 4)  # 10%–25%

    commission = round(gross_premium * commission_rate_pct, 2)
    ceded_commission = round(gross_premium * quota_share_pct * commission_rate_pct, 2)

    # For simplicity: net premium = gross_premium - ceded_commission
    # (you could also subtract commission if you prefer)
    net_premium = round(gross_premium - ceded_commission, 2)

    # Penal amount as 4–15x gross premium (surety-style)
    penal_multiplier = random.uniform(4, 15)
    penal_amount = round(gross_premium * penal_multiplier, 2)

    product = random.choice(PRODUCTS)
    broker_name = random.choice(BROKER_NAMES)
    broker_state = random.choice(STATES)
    obligee_name = random.choice(OBLIGEE_NAMES)
    obligee_state = random.choice(STATES)
    principal = random.choice(PRINCIPAL_NAMES)

    # Simple address pattern with the ZIP embedded
    principal_address = f"{random.randint(100, 9999)} {random.choice(['Main St', 'Solar Way', 'Energy Blvd', 'Renewal Ave'])}, {state} {zip_code}"

    row = {
        "Effective Date": eff.strftime("%Y-%m-%d"),
        "Expiration Date": exp.strftime("%Y-%m-%d"),
        "Gross Premium": gross_premium,
        "Quota Share %": quota_share_pct,
        "Commission Rate": commission_rate_pct,
        "Commission": commission,
        "Ceded Commission": ceded_commission,
        "Net Premium": net_premium,
        "Product": product,
        "Premium State": state,
        "Principal": principal,
        "Principal / Account Mailing Address": principal_address,
        "Penal Amount": penal_amount,
        "Broker Name": broker_name,
        "Broker State": broker_state,
        "Obligee Name": obligee_name,
        "Obligee State": obligee_state,
    }
    return row


def generate_carrier_bordereaux(carrier_name: str, n_rows: int) -> pd.DataFrame:
    """Generate a DataFrame of n_rows for a single carrier."""
    rows = [generate_one_row() for _ in range(n_rows)]
    df = pd.DataFrame(rows)

    # Small twist: slightly different distribution per carrier
    # e.g., tilt Alpha toward CA/WA, Beta toward PA/MA, Gamma toward TX/GA
    if carrier_name == "CarrierAlpha":
        df.loc[df.sample(frac=0.4).index, "Premium State"] = np.random.choice(["CA", "WA"], size=int(0.4 * len(df)))
    elif carrier_name == "CarrierBeta":
        df.loc[df.sample(frac=0.4).index, "Premium State"] = np.random.choice(["PA", "MA"], size=int(0.4 * len(df)))
    elif carrier_name == "CarrierGamma":
        df.loc[df.sample(frac=0.4).index, "Premium State"] = np.random.choice(["TX", "GA"], size=int(0.4 * len(df)))

    return df


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    for carrier_name, n_rows in CARRIERS:
        print(f"Generating {n_rows} rows for {carrier_name} ...")
        df = generate_carrier_bordereaux(carrier_name, n_rows)

        # Derive filename
        filename = f"{carrier_name}_Phase1_Bordereaux.xlsx"
        out_path = os.path.join(OUTPUT_DIR, filename)

        # Save to Excel
        df.to_excel(out_path, index=False)
        print(f"  -> Saved to {out_path} (rows: {len(df)})")

    print("\nDone. Realistic bordereaux files generated in data/raw/.")


if __name__ == "__main__":
    main()
