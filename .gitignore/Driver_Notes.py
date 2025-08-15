def attribution_notes(features_id: str, forecasts_id: str) -> list:
    feats = load_artifact(features_id)
    fc = load_artifact(forecasts_id)
    if (feats is None) or (fc is None) or getattr(feats, "empty", True) or getattr(fc, "empty", True):
        return ["Drivers unavailable"]

    # Minimal descriptive logic placeholder
    notes = []
    # Example: if promotions column exists and >0 near last periods → add a neutral note
    if "promo_intensity" in feats.columns and feats["promo_intensity"].fillna(0).sum() > 0:
        notes.append("Observed promotional activity in past periods; correlation not assessed.")
    # If seasonality flags exist (e.g., quarter) you can add a neutral observation
    if "quarter" in feats.columns:
        notes.append("Seasonal patterns may be present; detailed analysis pending.")
    return notes if notes else ["Drivers unavailable"]