import os 

def safe_to_csv(df, path, full_cfg, msg=None):
    global_cfg = full_cfg.get("global", {})
    qc_mode = global_cfg.get("qc_mode", True)
    if not qc_mode:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    df.to_csv(path, index=False)
    if msg:
        print(msg)
