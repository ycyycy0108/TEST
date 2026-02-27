"""
╔══════════════════════════════════════════════════════════════════════════╗
║  Neo PPC  v3.2.4  ―  SSOT 단일 통합 대시보드                            ║
╠══════════════════════════════════════════════════════════════════════════╣
║  CHANGELOG v3.2.3 → v3.2.4                                               ║
║                                                                            ║
║  [FIX] 1P 수익 공식 정정 (판매팀 미팅 + 1,160건 실증 검증)              ║
║   • FA(7%) 제거: DF 납품 방식에서는 Amazon이 FA를 차감하지 않음          ║
║     allowances = COOP(8%) + DA(3%) = 11% (기존 18% → 11%)               ║
║   • Net PPM 기반 도매가 산출: (1−NetPPM)/(1−allow) = 82%                ║
║   • 딜 funding 반영: factor = (1−w(1−a))/(1−deal_pct)                   ║
║     deal 10% → 채널비용 30%, deal 0% → 27% (= Net PPM)                  ║
║   • UI: wholesale_ratio 슬라이더 → Net PPM(27%) + 딜(10%) 슬라이더      ║
║   • 검증: 전체 MAE $0.66/unit, 편향 보수적(이익 과소 추정)              ║
║                                                                            ║
║  ⛔ BANNED (double counting risk):                                        ║
║     compute_profit, compute_be_roas, build_cost_model,                    ║
║     unit_fee, unit_ship, unit_ret, fee_rate, ship_per_order, ret_rate     ║
║                                                                            ║
║  Profit/BE 공식 (유일): operating_profit = sales - units×cogs_adj - spend ║
║      be_spend = sales - units×cogs_adj                                    ║
║      be_roas = sales / be_spend  |  be_acos = be_spend / sales × 100     ║
╚══════════════════════════════════════════════════════════════════════════╝
"""

# ════════════════════════════════════════════════════════════════════════
# 0. IMPORTS
# ════════════════════════════════════════════════════════════════════════
import os, glob, re
from io import StringIO, BytesIO
from datetime import datetime

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ════════════════════════════════════════════════════════════════════════
# 1. PAGE CONFIG + CSS
# ════════════════════════════════════════════════════════════════════════
st.set_page_config(page_title="Neo PPC v3.2", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Pretendard:wght@400;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Pretendard',sans-serif;}
.kpi-card{background:#fff;border:1px solid #E2E8F0;border-radius:10px;
  padding:14px 16px 10px;box-shadow:0 1px 4px rgba(0,0,0,.05);min-height:82px;}
.kpi-card.c-blue{border-top:3px solid #3B82F6;}
.kpi-card.c-green{border-top:3px solid #10B981;}
.kpi-card.c-amber{border-top:3px solid #F59E0B;}
.kpi-card.c-red{border-top:3px solid #EF4444;}
.kpi-card.c-purple{border-top:3px solid #8B5CF6;}
.kpi-card.c-teal{border-top:3px solid #14B8A6;}
.kpi-card.c-slate{border-top:3px solid #64748B;}
.kpi-label{font-size:10px;font-weight:700;letter-spacing:1.1px;
  color:#64748B;text-transform:uppercase;margin-bottom:4px;}
.kpi-value{font-size:21px;font-weight:700;color:#0F172A;
  font-family:'IBM Plex Mono',monospace;line-height:1.2;}
.kpi-sub{font-size:11px;color:#94A3B8;margin-top:3px;}
.kpi-good{font-size:11px;color:#16A34A;margin-top:3px;font-weight:600;}
.kpi-bad{font-size:11px;color:#DC2626;margin-top:3px;font-weight:600;}
.kpi-warn{font-size:11px;color:#D97706;margin-top:3px;font-weight:600;}
.sec-hdr{font-size:13px;font-weight:700;color:#1E293B;
  border-left:3px solid #3B82F6;padding:2px 0 2px 10px;margin:18px 0 8px;}
.insight-box{background:#FFFBEB;border-left:4px solid #F59E0B;
  padding:12px 16px;margin:8px 0;border-radius:0 6px 6px 0;font-size:13px;}
[data-testid="stSidebar"]{background:#F8FAFC;border-right:1px solid #E2E8F0;}
</style>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════════
# 2. CONSTANTS
# ════════════════════════════════════════════════════════════════════════
DATA_DIR = os.path.dirname(os.path.abspath(__file__))

_COL_MAP = {
    "Date":"date","Campaign name":"campaign","Ad group name":"ad_group",
    "Advertised product SKU":"sku","Advertised product ID":"asin",
    "Placement classification":"placement","Target match type":"match_type",
    "Target value":"target_value","Search term":"search_term",
    "Impressions":"impressions","Clicks":"clicks","Total cost":"spend",
    "Purchases":"orders","Units sold":"units","Sales":"sales",
    "Long-term sales":"long_term_sales","Advertiser account name":"account_name",
}
_SUM_COLS = ["impressions","clicks","spend","orders","units","sales","long_term_sales"]
_DEDUP_KEY= ["date","campaign","ad_group","asin","search_term","placement","target_value"]

_PLACEMENT_LABEL = {
    "Top of Search on-Amazon":"검색 상단","Other on-Amazon":"검색 기타",
    "Detail Page on-Amazon":"상품 상세","Off Amazon":"외부(Off)",
}
_TARGET_STD = {
    "close-match":"근접 매칭","loose-match":"유사 매칭",
    "substitutes":"대체 상품","complements":"보완 상품",
    "Automatic - Close match":"근접 매칭","Automatic - Loose match":"유사 매칭",
    "Automatic - Substitutes":"대체 상품","Automatic - Complements":"보완 상품",
}
_TARGET_STD_KO = set(_TARGET_STD.values())

C = dict(
    spend="#3B82F6",sales="#10B981",acos="#F59E0B",roas="#8B5CF6",
    cpc="#EF4444",lt="#14B8A6",
    profit="#10B981",loss="#EF4444",
    bg_good="#DCFCE7",bg_warn="#FEF9C3",bg_bad="#FEE2E2",
    row_good="#F0FDF4",row_warn="#FEFCE8",row_bad="#FEF2F2",
)
BASE_LAYOUT = dict(
    plot_bgcolor="#FFFFFF",paper_bgcolor="#F8FAFC",
    font=dict(color="#374151",size=11,family="Pretendard"),
    hovermode="x unified",
    legend=dict(orientation="v",x=1.01,y=1,xanchor="left",
                font=dict(size=11),bgcolor="rgba(255,255,255,.85)",
                bordercolor="#E2E8F0",borderwidth=1),
    margin=dict(l=4,r=140,t=40,b=4),
    xaxis=dict(gridcolor="#E5E7EB",zeroline=False),
    yaxis=dict(gridcolor="#E5E7EB",zeroline=False),
)
SUM_COLS = _SUM_COLS  # alias used by some v3 blocks

# ════════════════════════════════════════════════════════════════════════
# 3. FORMAT + UI HELPERS
# ════════════════════════════════════════════════════════════════════════
def fu(v) -> str:  return f"${v:,.2f}"  if pd.notna(v) else "$0.00"
def fp(v) -> str:  return f"{v:.1f}%"   if pd.notna(v) else "0.0%"
def fx(v) -> str:  return f"{v:.2f}x"   if pd.notna(v) else "0.00x"
def fi(v) -> str:  return f"{int(v):,}" if pd.notna(v) else "0"

def safe_div(n, d, pct=False):
    try:
        if d==0 or pd.isna(d) or pd.isna(n): return 0.0
        r = float(n)/float(d)
        return r*100. if pct else r
    except: return 0.0

def sec(title):
    st.markdown(f'<div class="sec-hdr">{title}</div>', unsafe_allow_html=True)

def kpi_card(col, label, value, sub="", sub_cls="kpi-sub", color="blue"):
    sub_html = f'<div class="{sub_cls}">{sub}</div>' if sub else ""
    col.markdown(
        f'<div class="kpi-card c-{color}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>{sub_html}</div>',
        unsafe_allow_html=True)

def chart_guide(md):
    with st.expander("📖 차트 해석 가이드", expanded=False):
        st.markdown(md)

def sort_df(df, metric, asc):
    if metric in df.columns:
        return df.sort_values(metric, ascending=asc).reset_index(drop=True)
    return df

def acos_bg(v, ok, warn):
    if v<=ok: return C["bg_good"]
    if v<=warn: return C["bg_warn"]
    return C["bg_bad"]

def verdict_label(roas, be):
    """수익 중심: ROAS vs BE 기준"""
    if roas >= be*1.2: return "🟢 SCALE"
    if roas >= be*0.9: return "🟡 HOLD"
    if roas >= be*0.7: return "🔧 FIX"
    return "🔴 CUT"

def verdict_label_growth(acos_pct, target_acos):
    """성장 중심: ACoS vs 목표 기준"""
    if acos_pct <= target_acos:         return "🟢 SCALE"
    if acos_pct <= target_acos * 1.3:   return "🟡 HOLD"
    if acos_pct <= target_acos * 2.0:   return "🔧 FIX"
    return "🔴 CUT"

def _apply_verdict(row, be_roas, is_growth, target_acos):
    """전략 모드에 따라 적절한 verdict 반환"""
    if is_growth:
        return verdict_label_growth(row.get("acos_pct", 999), target_acos)
    return verdict_label(row.get("roas", 0), be_roas)

def tier_label(roas, profit, orders, spend, avg_spend, be):
    if orders==0 and spend>0: return "T4 🚫 차단"
    if roas>=be*1.2 and profit>0 and orders>=2 and spend>=avg_spend*0.5: return "T1 ⭐ 확장"
    if roas<be*0.8 and profit<0: return "T3 🔴 적자"
    return "T2 🟡 유지"

def _classify_kw(term):
    brand_kws=["neochair","neo chair","neo sleep","mlw","801","nec","pace","blc",
               "titan","apex","czpu","czw","hpac","cub","cntt","cozy","mesh801"]
    comp_kws=["hbada","sihoo","secretlab","autonomous","branch","herman miller",
              "steelcase","haworth","ergochair","dxracer","respawn","nouhaus"]
    t=str(term).lower()
    for b in brand_kws:
        if b in t: return "🏷 Brand"
    for c in comp_kws:
        if c in t: return "⚔ Competitor"
    return "🔍 Generic"

def _classify_target(val):
    if pd.isna(val) or str(val).strip()=="": return "수동 키워드"
    v=str(val).strip()
    ko=_TARGET_STD.get(v)
    if ko: return ko
    if v in _TARGET_STD_KO: return v
    return "수동 키워드"

def _classify_purpose(name):
    n=str(name).lower()
    if "brand" in n: return "🏷 Brand"
    if "keyword" in n or "manual" in n or "exact" in n: return "🎯 Manual"
    if "b2b" in n: return "🏢 B2B"
    if "24hr" in n or "24h" in n: return "⏰ 24hr"
    if "auto" in n: return "🤖 Auto"
    return "📦 기타"


# ════════════════════════════════════════════════════════════════════════
# 4. EXTRACT LAYER
# ════════════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="📊 Ads SSOT 로드 중…")
def extract_fact_ads(raw_bytes: bytes) -> pd.DataFrame:
    df = pd.read_csv(StringIO(raw_bytes.decode("utf-8-sig")), low_memory=False)
    df.columns = df.columns.str.strip()
    for c in ["Impressions","Clicks","Total cost","Purchases","Units sold","Sales","Long-term sales"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], format="%b %d, %Y", errors="coerce")
        df.dropna(subset=["Date"], inplace=True)
    df = df.rename(columns={k:v for k,v in _COL_MAP.items() if k in df.columns})
    df["campaign_type"] = np.where(df["campaign"].str.contains("Auto",case=False,na=False),"Auto","Manual")
    df["_campaign_type"] = df["campaign_type"]   # alias for v3 blocks
    if "placement" in df.columns:
        df["placement"] = df["placement"].map(_PLACEMENT_LABEL).fillna(df["placement"])
    df["target_group"] = df.get("target_value", pd.Series(dtype=str)).apply(_classify_target) if "target_value" in df.columns else "수동 키워드"
    for c in _SUM_COLS + ["asin","sku","search_term","placement","ad_group","target_value","long_term_sales"]:
        if c not in df.columns: df[c] = np.nan if c not in _SUM_COLS else 0.0
    # Normalise ASIN / SKU
    df["asin"] = df["asin"].astype(str).str.strip().str.upper().replace("NAN","")
    df["sku"]  = df["sku"].astype(str).str.strip().str.upper().replace("NAN","")
    df.loc[df["asin"]=="","asin"] = np.nan
    df.loc[df["sku"]=="","sku"]   = np.nan
    # FIX: amzn1 SKU → ASIN으로 대체 (NaN이면 테이블에서 사라지므로)
    _amzn_mask = df["sku"].str.startswith("AMZN", na=False)
    df.loc[_amzn_mask, "sku"] = df.loc[_amzn_mask, "asin"]
    df["channel"] = "SSOT"   # v3 blocks reference df["channel"]
    # ── 1P / 3P 판매 채널 분류 (캠페인명 "1P" prefix → 1P, else 3P) ──
    _has_campaign = "campaign" in df.columns and df["campaign"].notna().any()
    df["sales_channel"] = np.where(
        df["campaign"].str.startswith("1P", na=False), "1P", "3P"
    ) if _has_campaign else "3P"
    key = [c for c in _DEDUP_KEY if c in df.columns]
    return df.drop_duplicates(subset=key).reset_index(drop=True)


@st.cache_data(show_spinner="🏷 원가 마스터 로드 중…")
def extract_cogs(raw_bytes: bytes, ext: str) -> pd.DataFrame:
    """Parse cost master → DataFrame[asin, sku_cogs, unit_cogs]. Join key: ASIN."""
    if ext in ("xlsx","xls"): raw = pd.read_excel(BytesIO(raw_bytes))
    else: raw = pd.read_csv(StringIO(raw_bytes.decode("utf-8-sig")))
    raw.columns = raw.columns.str.strip()
    low2orig = {c.lower():c for c in raw.columns}
    asin_orig = (low2orig.get("asin") or low2orig.get("advertised product id")
                 or next((v for k,v in low2orig.items() if "asin" in k), None))
    sku_orig  = (low2orig.get("itm_id") or low2orig.get("sku")
                 or next((v for k,v in low2orig.items() if "sku" in k or "itm" in k), None))
    cost_orig = (low2orig.get("원가") or low2orig.get("unit_cogs") or low2orig.get("cogs")
                 or next((v for k,v in low2orig.items() if "원가" in k or "cogs" in k), None))
    if asin_orig is None:
        st.warning("⚠ 원가 파일에서 ASIN 컬럼을 찾지 못했습니다."); return pd.DataFrame(columns=["asin","sku_cogs","unit_cogs"])
    if cost_orig is None:
        st.warning("⚠ 원가 파일에서 원가 컬럼을 찾지 못했습니다."); return pd.DataFrame(columns=["asin","sku_cogs","unit_cogs"])
    keep = [asin_orig, cost_orig] + ([sku_orig] if sku_orig and sku_orig!=asin_orig else [])
    df = raw[keep].copy()
    df.columns = ["asin","unit_cogs","sku_cogs"] if len(keep)==3 else ["asin","unit_cogs"]
    if "sku_cogs" not in df.columns: df["sku_cogs"] = np.nan
    df["asin"] = df["asin"].astype(str).str.strip().str.upper()
    df["unit_cogs"] = pd.to_numeric(df["unit_cogs"], errors="coerce")
    # ── FIX: 원가 NaN인 ASIN에 대해 같은 제품군(ITM_ID 접두사) 원가로 추정 ──
    if "sku_cogs" in df.columns:
        _nan_mask = df["unit_cogs"].isna() & df["sku_cogs"].notna()
        _has_cost = df[df["unit_cogs"].notna()].copy()
        if not _has_cost.empty and _nan_mask.any():
            # ITM_ID에서 사이즈 코드를 추출 → 같은 제품군의 원가 찾기
            def _estimate_from_family(itm_id):
                if pd.isna(itm_id): return np.nan
                itm = str(itm_id).strip()
                # 접두사 기반 검색: 마지막 '-' 전까지 → 같은 접두사 + 원가 있는 행
                parts = itm.rsplit("-", 1)
                if len(parts) < 2: return np.nan
                prefix = parts[0]
                family = _has_cost[_has_cost["sku_cogs"].astype(str).str.startswith(prefix, na=False)]
                if not family.empty:
                    return family["unit_cogs"].median()
                # 더 짧은 접두사로 재시도
                parts2 = prefix.rsplit("-", 1)
                if len(parts2) < 2: return np.nan
                family2 = _has_cost[_has_cost["sku_cogs"].astype(str).str.startswith(parts2[0], na=False)]
                return family2["unit_cogs"].median() if not family2.empty else np.nan
            df.loc[_nan_mask, "unit_cogs"] = df.loc[_nan_mask, "sku_cogs"].apply(_estimate_from_family)
            _filled = _nan_mask & df["unit_cogs"].notna()
            if _filled.any():
                st.info(f"ℹ 원가 NaN {_filled.sum()}개 ASIN에 제품군 추정원가 적용됨")
    df = df[df["unit_cogs"].notna() & (df["asin"]!="") & (df["asin"]!="NAN")]
    df = df.sort_values("unit_cogs",ascending=False).drop_duplicates("asin",keep="first")
    return df[["asin","sku_cogs","unit_cogs"]].reset_index(drop=True)


def _find_cogs_file(directory):
    for pat in ["*unit_cogs*.xlsx","*unit_cogs*.xls","*cogs*.xlsx","*원가*.xlsx"]:
        hits = glob.glob(os.path.join(directory,pat))
        if hits: return max(hits,key=os.path.getmtime)
    return None

def _find_ads_files(directory):
    hits = []
    for pat in ["PPC_analysis*.csv","*PPC*.csv","*AMAZON*.csv","*ADS*.csv"]:
        hits.extend(glob.glob(os.path.join(directory,pat)))
    return sorted(set(hits),key=os.path.getmtime,reverse=True)


# ════════════════════════════════════════════════════════════════════════
# 5. TRANSFORM LAYER  — canonical profit model
# ════════════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner="🔗 ASIN 원가 병합 중…")
def transform_merge(fact: pd.DataFrame, cogs: pd.DataFrame) -> pd.DataFrame:
    """LEFT JOIN on ASIN — the only join key in v3.1."""
    # ── 1. Drop stale cogs columns from fact to prevent _x/_y duplicates ──
    _drop = [c for c in ["unit_cogs", "sku_cogs", "cogs_source"] if c in fact.columns]
    f = fact.drop(columns=_drop).copy() if _drop else fact.copy()

    # ── 2. Empty cogs → proxy mode ───────────────────────────────────────
    if cogs.empty:
        f["unit_cogs"]   = np.nan
        f["sku_cogs"]    = np.nan
        f["cogs_source"] = "proxy"
        return f

    # ── 3. Merge only required columns from cogs ─────────────────────────
    m = f.merge(cogs[["asin", "unit_cogs", "sku_cogs"]], on="asin", how="left")
    m["cogs_source"] = np.where(m["unit_cogs"].notna(), "actual", "proxy")

    # ── 4. FIX: ASIN형 SKU → 실제 SKU 역매핑 (sku_cogs에서 가져옴) ──
    # sku_cogs = COGS ITM_ID 컬럼 (실제 SKU 코드)
    # sku가 ASIN 형식(B0*)이고, sku_cogs에 실제 SKU가 있으면 대체
    if "sku_cogs" in m.columns:
        _asin_sku_mask = (
            m["sku"].str.startswith("B0", na=False) &
            m["sku_cogs"].notna() &
            ~m["sku_cogs"].astype(str).str.startswith("B0", na=False) &
            ~m["sku_cogs"].astype(str).str.startswith("AMZN", na=False) &
            (m["sku_cogs"].astype(str).str.strip() != "")
        )
        m.loc[_asin_sku_mask, "sku"] = m.loc[_asin_sku_mask, "sku_cogs"].str.upper()

    return m


def transform_add_profit(df: pd.DataFrame, cost_mult: float,
                         amazon_fee_pct: float = 0.0,
                         fba_ship_per_unit: float = 0.0,
                         return_pct: float = 0.0,
                         coop_pct: float = 0.0,
                         da_pct: float = 0.0,
                         fa_pct: float = 0.0,
                         wholesale_ratio: float = 0.82,
                         deal_pct: float = 0.10) -> pd.DataFrame:
    """
    CANONICAL PROFIT MODEL — used by ALL tabs.  (v3.2.4)

    Row-level 1P / 3P cost gating via ``sales_channel`` column.
    If ``sales_channel`` is absent, all rows are treated as 3P (backward-compat).

    ── 1P rows (Amazon wholesale, DF fulfillment) ─────────────────────
    PPC Sales = consumer retail price (deal price if deal is active).
    Channel deduction formula:

      factor = (1 - wholesale_ratio × (1 - COOP - DA)) / (1 - deal_pct)
      oneP_channel_deduction = avg_price × factor

    This accounts for:
      • Amazon retail margin (1 - wholesale_ratio)
      • COOP + DA allowances on wholesale price
      • Deal funding = (base_price - deal_price), reversed from avg_price
    FA is intentionally excluded — DF fulfillment means no FA charge.

    ── 3P rows (direct sell) ───────────────────────────────────────────
      channel_cost = avg_price × amazon_fee_pct + fba_ship_per_unit

    ── Common ──────────────────────────────────────────────────────────
      base_cogs    = unit_cogs × (1 + cost_mult)  [or fallback avg_price×0.70]
      return_cost  = avg_price × return_pct × 0.50

    Final (unchanged canonical shape):
      unit_cogs_adj    = base_cogs + channel_cost + return_cost
      operating_profit = sales − units × unit_cogs_adj − spend
    """
    d = df.copy()
    d["_apr"] = np.where(d["units"] > 0, d["sales"] / d["units"], 0.0)
    fallback = d["_apr"] * 0.70

    # ── 제품 원가 (Landed Cost) with cost_mult ──
    base_cogs = np.where(d["unit_cogs"].notna(),
                         d["unit_cogs"] * (1 + cost_mult),
                         fallback * (1 + cost_mult))

    # ── 채널 판별 (없으면 전부 3P — backward compat) ──
    is_1p = (d["sales_channel"] == "1P").values if "sales_channel" in d.columns \
            else np.zeros(len(d), dtype=bool)

    # ── 1P 채널 비용: Net PPM + deal funding correction ──
    # FA excluded from 1P under DF fulfillment (fa_pct NOT used here)
    oneP_allow = coop_pct + da_pct  # COOP + DA only, no FA
    _denom = max(1.0 - deal_pct, 0.01)  # guard div-by-zero
    oneP_deduction = d["_apr"] * (1.0 - wholesale_ratio * (1.0 - oneP_allow)) / _denom

    # ── 3P 채널 비용: Referral Fee + FBA ──
    threeP_cost = d["_apr"] * amazon_fee_pct + fba_ship_per_unit

    # ── 행 레벨 채널 비용 선택 ──
    channel_cost = np.where(is_1p, oneP_deduction, threeP_cost)

    # ── 반품 손실 (1P/3P 공통) ──
    return_cost = d["_apr"] * return_pct * 0.50

    # ── 통합 단위원가 → canonical profit ──
    d["unit_cogs_adj"]    = base_cogs + channel_cost + return_cost
    d["is_proxy_cost"]    = d["unit_cogs"].isna()
    d["operating_profit"] = d["sales"] - d["units"] * d["unit_cogs_adj"] - d["spend"]
    d["profit"]           = d["operating_profit"]   # alias for v3 blocks
    d["be_spend"]         = d["sales"] - d["units"] * d["unit_cogs_adj"]
    d["be_roas"]          = np.where(d["be_spend"] > 0, d["sales"] / d["be_spend"], np.inf)
    d["be_acos"]          = np.where((d["be_spend"] > 0) & (d["sales"] > 0),
                                      d["be_spend"] / d["sales"] * 100, np.nan)
    d["structural_loss"]  = d["be_spend"] <= 0
    d.drop(columns=["_apr"], inplace=True, errors="ignore")
    return d


@st.cache_data(show_spinner="📐 KPI 집계 중…")
def agg_kpis(df: pd.DataFrame, group_cols: list) -> pd.DataFrame:
    extra = ["unit_cogs_adj","operating_profit","profit","is_proxy_cost","be_spend","long_term_sales"]
    agg_map = {c:"sum" for c in _SUM_COLS+extra if c in df.columns}
    g = df.groupby(group_cols, dropna=False).agg(agg_map).reset_index()
    return _recalc_ratios(g)

# Alias used by v3 blocks
def agg_fact(df, group_cols):
    return agg_kpis(df, group_cols)


def _recalc_ratios(g):
    sp=g["spend"]; sa=g["sales"]; cl=g["clicks"]
    im=g["impressions"]; or_=g["orders"]; un=g["units"]
    g["roas"]        = sa.div(sp.replace(0,np.nan)).fillna(0)
    g["acos_pct"]    = sp.div(sa.replace(0,np.nan)).fillna(0)*100
    g["cpc"]         = sp.div(cl.replace(0,np.nan)).fillna(0)
    g["ctr_pct"]     = cl.div(im.replace(0,np.nan)).fillna(0)*100
    g["cvr_ord_pct"] = or_.div(cl.replace(0,np.nan)).fillna(0)*100
    g["cvr_pct"]     = g["cvr_ord_pct"]   # alias
    g["aov"]         = sa.div(or_.replace(0,np.nan)).fillna(0)
    g["avg_price"]   = sa.div(un.replace(0,np.nan)).fillna(0)
    g["cpa"]         = sp.div(or_.replace(0,np.nan)).fillna(0)
    g["cpa_unit"]    = g["cpa"]   # alias
    g["cpa_ord"]     = g["cpa"]
    g["ord_per_impr"]= or_.div(im.replace(0,np.nan)).fillna(0)*100
    g["ctr"]         = g["ctr_pct"]   # alias
    if "be_spend" in g.columns:
        g["be_roas"] = np.where(g["be_spend"]>0, g["sales"]/g["be_spend"], np.inf)
        g["be_acos"] = np.where((g["be_spend"]>0)&(g["sales"]>0),
                                 g["be_spend"]/g["sales"]*100, np.nan)
        g["structural_loss"] = g["be_spend"]<=0
    return g


def cogs_coverage(df):
    if "unit_cogs" not in df.columns:
        return dict(total_asin=0,matched_asin=0,match_pct=0.,spend_matched_pct=0.)
    has = df[df["asin"].notna()&(df["asin"]!="")]
    total = has["asin"].nunique()
    matched = has[has["unit_cogs"].notna()]["asin"].nunique()
    t_sp = has["spend"].sum(); m_sp = has[has["unit_cogs"].notna()]["spend"].sum()
    return dict(total_asin=total,matched_asin=matched,
                match_pct=safe_div(matched,total,pct=True),
                spend_matched_pct=safe_div(m_sp,t_sp,pct=True))

# Alias — v3 blocks call cogs_match_rate
def cogs_match_rate(df):
    cov = cogs_coverage(df)
    return dict(match_pct=cov["match_pct"],
                matched_skus=cov["matched_asin"],
                total_skus=cov["total_asin"],
                matched_spend_pct=cov["spend_matched_pct"])


def enrich_be_acos(df, fee_rate=0.0, ship_per_order=0.0):
    """
    Attach BE-ACoS columns using canonical model.
    fee_rate / ship_per_order params are kept for API compatibility but
    in v3.1 unit_cogs is ALL-IN, so they default to 0.
    """
    d = df.copy()
    d["acos"] = np.where(d["sales"]>0, d["spend"]/d["sales"], np.nan)
    d["cogs_total"]    = d["units"] * d["unit_cogs"].fillna(d.get("avg_price",d["sales"].div(d["units"].replace(0,np.nan)))*0.7)
    d["fee_total"]     = d["sales"] * fee_rate
    d["ship_total"]    = d["orders"] * ship_per_order
    d["contrib_ex_ad"] = d["sales"] - d["cogs_total"] - d["fee_total"] - d["ship_total"]
    d["be_acos_pct"]   = np.where(d["sales"]>0, d["contrib_ex_ad"]/d["sales"], np.nan)
    d["acos_gap"]      = np.where(d["sales"]>0, d["acos"] - d["be_acos_pct"].clip(lower=0), np.nan)
    return d


def acos_gap_summary(df, group_col="sku", fee_rate=0.0, ship_per_order=0.0, top_n=20):
    agg_cols = {c:"sum" for c in ["spend","sales","orders","units","clicks","impressions"]
                if c in df.columns}
    if "unit_cogs" in df.columns: agg_cols["unit_cogs"] = "first"
    g = df.groupby(group_col, as_index=False).agg(agg_cols)
    g["acos"] = np.where(g["sales"]>0, g["spend"]/g["sales"], np.nan)
    g["cogs_total"] = g["units"] * g.get("unit_cogs", pd.Series(0, index=g.index)).fillna(0)
    g["contrib"] = g["sales"] - g["cogs_total"] - g["sales"]*fee_rate - g["orders"]*ship_per_order
    g["be_acos"]  = np.where(g["sales"]>0, g["contrib"]/g["sales"], np.nan)
    g["acos_gap"] = g["acos"] - g["be_acos"].clip(lower=0)
    g["roas"]     = np.where(g["spend"]>0, g["sales"]/g["spend"], 0)
    g["cpc"]      = np.where(g["clicks"]>0, g["spend"]/g["clicks"], 0)
    return g.sort_values("acos_gap", ascending=False).head(top_n).reset_index(drop=True)


# ── Compatibility shim: old compute_profit() calls in v3 tab blocks ──
def _compute_profit_inline(sales, orders, spend, margin=0.35, fee=0.0, ship=0.0, ret_rate=0.0):
    """
    v3.1 shim: unit_cogs is ALL-IN, so fee/ship/ret = 0.
    Returns rough proxy: sales*margin - spend.
    Real profit is always in df["operating_profit"].
    """
    return sales * margin - spend


# ════════════════════════════════════════════════════════════════════════
# 6. UI CHART HELPERS
# ════════════════════════════════════════════════════════════════════════
def bar_line_fig(x, bars, line_key=None, title="", height=300):
    use_sec = line_key is not None
    fig = make_subplots(specs=[[{"secondary_y":use_sec}]]) if use_sec else go.Figure()
    bar_count = sum(1 for name in bars if not (use_sec and name==line_key))
    for name,(yvals,color) in bars.items():
        is_line = use_sec and name==line_key
        if is_line:
            tr = go.Scatter(x=x,y=yvals,name=name,mode="lines+markers",
                            line=dict(color=color,width=2.5),marker=dict(size=5))
            fig.add_trace(tr,secondary_y=True)
        else:
            # 바가 2개 이상이면 inside+auto 로 겹침 방지
            _tpos = "inside" if bar_count >= 2 else "outside"
            tr = go.Bar(x=x,y=yvals,name=name,marker_color=color,opacity=.85,
                        text=yvals.map(lambda v: f"${v:,.0f}"),
                        textposition=_tpos,
                        insidetextanchor="middle",
                        constraintext="both",
                        textfont=dict(size=9,color="white" if bar_count>=2 else "#374151"))
            if use_sec: fig.add_trace(tr,secondary_y=False)
            else:       fig.add_trace(tr)
    fig.update_layout(**{**BASE_LAYOUT,"barmode":"group","height":height,"title":title,
                         "uniformtext":dict(minsize=7,mode="hide")})
    if use_sec:
        fig.update_yaxes(title_text="USD ($)",secondary_y=False)
        fig.update_yaxes(title_text="ACoS (%)",secondary_y=True,gridcolor=None)
    return fig


def build_campaign_table_fig(cg, target_acos):
    def rc(v):
        if v<=target_acos: return C["row_good"]
        if v<=target_acos*1.5: return C["row_warn"]
        return C["row_bad"]
    colors=[rc(v) for v in cg["acos_pct"]]
    cols=["campaign","campaign_type","spend","sales","orders","units","acos_pct","roas","cpc","ctr_pct","cvr_pct","aov","cpa"]
    cols=[c for c in cols if c in cg.columns]
    labels={"campaign":"캠페인","campaign_type":"유형","spend":"Spend($)","sales":"Sales($)",
            "orders":"주문","units":"판매","acos_pct":"ACoS(%)","roas":"ROAS(x)",
            "cpc":"CPC($)","ctr_pct":"CTR(%)","cvr_pct":"CVR(%)","aov":"AOV($)","cpa":"CPA($)"}
    fmts={"spend":fu,"sales":fu,"acos_pct":fp,"roas":fx,"cpc":fu,"ctr_pct":fp,"cvr_pct":fp,"aov":fu,"cpa":fu,"orders":fi,"units":fi}
    values=[]
    for c in cols:
        if c in fmts: values.append(cg[c].map(fmts[c]))
        else:         values.append(cg[c])
    fig=go.Figure(go.Table(
        columnwidth=[160]+[45]*(len(cols)-1),
        header=dict(values=[f"<b>{labels.get(c,c)}</b>" for c in cols],
                    fill_color="#1E293B",font=dict(color="white",size=10),align="center",height=30),
        cells=dict(values=values,fill_color=[colors]*len(cols),
                   font=dict(color="#1E293B",size=10),align="center",height=26)))
    fig.update_layout(height=max(220,len(cg)*28+80),margin=dict(l=0,r=0,t=4,b=0))
    return fig


# ════════════════════════════════════════════════════════════════════════
# 7. SIDEBAR + DATA LOADING
# ════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 📊 Neo PPC v3.2")
    st.caption("SSOT 단일 소스 · ASIN 원가 조인 · 단일 Profit 모델")
    st.divider()
    st.markdown("**① Amazon Ads SSOT CSV** `필수`")
    up_ads = st.file_uploader("", type=["csv"], accept_multiple_files=True,
                               label_visibility="collapsed", key="up_ads")
    st.markdown("**② 원가 마스터 (ASIN + 원가)** `자동탐지 or 업로드`")
    up_cogs = st.file_uploader("", type=["csv","xlsx","xls"],
                                label_visibility="collapsed", key="up_cogs")
    st.divider()
    admin_mode = st.toggle("🔧 Admin 모드 (원가 신뢰도 탭)", value=False, key="admin_mode")
    st.divider()

# ── Fact loading ──────────────────────────────────────────────────────
fact_frames = []
if up_ads:
    for f in up_ads:
        try: fact_frames.append(extract_fact_ads(f.read()))
        except Exception as e: st.sidebar.error(f"❌ {f.name}: {e}")
else:
    for fpath in _find_ads_files(DATA_DIR):
        try:
            with open(fpath,"rb") as fh: fact_frames.append(extract_fact_ads(fh.read()))
            st.sidebar.caption(f"📄 자동: {os.path.basename(fpath)}")
        except: pass

if not fact_frames:
    st.info("👈 사이드바에서 Amazon Ads CSV를 업로드하거나, 같은 폴더에 PPC_analysis*.csv를 위치시키세요.")
    st.stop()

fact_raw = pd.concat(fact_frames, ignore_index=True)
fact_raw["date"] = pd.to_datetime(fact_raw["date"], errors="coerce")
fact_raw = fact_raw.dropna(subset=["date"]).reset_index(drop=True)
fact_raw = fact_raw.drop_duplicates(subset=[c for c in _DEDUP_KEY if c in fact_raw.columns]).reset_index(drop=True)

# ── Cost loading ──────────────────────────────────────────────────────
cogs_df = pd.DataFrame(columns=["asin","sku_cogs","unit_cogs"])
if up_cogs:
    ext = up_cogs.name.rsplit(".",1)[-1].lower()
    try:
        cogs_df = extract_cogs(up_cogs.read(), ext)
        st.sidebar.success(f"✅ 원가 업로드 ({len(cogs_df)} ASIN)")
    except Exception as e: st.sidebar.warning(f"⚠ 원가: {e}")
else:
    cp = _find_cogs_file(DATA_DIR)
    if cp:
        ext = cp.rsplit(".",1)[-1].lower()
        try:
            with open(cp,"rb") as fh: cogs_df = extract_cogs(fh.read(), ext)
            st.sidebar.caption(f"🏷 원가 자동: {os.path.basename(cp)}")
        except: pass
    else:
        st.sidebar.info("ℹ 원가 파일 미탐지 — Proxy 모드")

HAS_COGS = not cogs_df.empty

# ── ASIN-based merge (once) ───────────────────────────────────────────
fact_merged_base = transform_merge(fact_raw, cogs_df)

# ── ASIN coverage report ──────────────────────────────────────────────
_cov = cogs_coverage(fact_merged_base)
if HAS_COGS:
    _icon = "✅" if _cov["match_pct"]>=80 else ("⚠" if _cov["match_pct"]>=50 else "❌")
    st.sidebar.caption(f"💰 ASIN 매칭: {_icon} {_cov['matched_asin']}/{_cov['total_asin']} ({_cov['match_pct']:.0f}%) · Spend {_cov['spend_matched_pct']:.0f}%")
    if _cov["match_pct"]<70:
        _miss = fact_merged_base[fact_merged_base["unit_cogs"].isna()&fact_merged_base["asin"].notna()]
        if not _miss.empty:
            _top5 = _miss.groupby("asin")["spend"].sum().sort_values(ascending=False).head(5).index.tolist()
            st.sidebar.warning("❌ 미매칭 상위 ASIN:\n" + ", ".join(_top5))

# ════════════════════════════════════════════════════════════════════════
# 8. SIDEBAR GLOBAL FILTERS
# ════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("**📅 조회 기간**")
    _date_series = fact_raw["date"].dropna()
    if _date_series.empty:
        st.error("❌ Date parsing failed: check Ads CSV date column format.")
        st.stop()
    _dmin = _date_series.min().date()
    _dmax = _date_series.max().date()
    dr = st.date_input("", value=(_dmin,_dmax), min_value=_dmin, max_value=_dmax,
                       label_visibility="collapsed", key="g_date")
    D0 = pd.Timestamp(dr[0]) if len(dr)>=1 else pd.Timestamp(_dmin)
    D1 = pd.Timestamp(dr[1]) if len(dr)==2 else pd.Timestamp(_dmax)

    st.markdown("**⚙ 캠페인 유형**")
    _type_opts = sorted(fact_merged_base["campaign_type"].dropna().unique())
    sel_type = st.multiselect("",_type_opts,default=_type_opts,
                               label_visibility="collapsed",key="g_type")

    st.markdown("**🎯 캠페인**")
    _camp_opts = sorted(fact_merged_base["campaign"].dropna().unique())
    sel_camp = st.multiselect("",_camp_opts,default=_camp_opts,
                               label_visibility="collapsed",placeholder="전체",key="g_camp")

    st.markdown("**📦 SKU**")
    _sku_opts = sorted(fact_merged_base["sku"].dropna().unique())
    sel_sku = st.multiselect("",_sku_opts,default=_sku_opts,
                              label_visibility="collapsed",placeholder="전체",key="g_sku")

    st.divider()
    st.markdown("**🎯 목표 ACoS (%)**")
    TARGET_ACOS = st.number_input("",value=15.0,min_value=5.0,max_value=60.0,
                                   step=1.0,label_visibility="collapsed",key="g_acos")
    ACOS_WARN = TARGET_ACOS * 1.5

    st.divider()
    st.markdown("**🎯 PPC 전략 모드**")
    PPC_MODE = st.radio(
        "", ["💰 수익 중심", "📈 성장 중심"],
        label_visibility="collapsed", key="g_ppc_mode",
    )
    IS_GROWTH_MODE = (PPC_MODE == "📈 성장 중심")
    if IS_GROWTH_MODE:
        st.markdown(
            '<div style="background:#EFF6FF;padding:8px 10px;border-radius:6px;font-size:12px;line-height:1.5">'
            '<b>📈 성장 중심</b> (신제품·런칭)<br>'
            '• 핵심 KPI: <b>ACoS, CTR, CVR, CPC</b><br>'
            '• 판단: ACoS가 목표 이내? 광고 효율 개선 추세?<br>'
            '• Verdict: ACoS ≤ 목표 → SCALE | ×1.3 → HOLD | ×2 → FIX | 초과 → CUT<br>'
            '• 적자여도 효율 개선 중이면 OK'
            '</div>', unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div style="background:#F0FDF4;padding:8px 10px;border-radius:6px;font-size:12px;line-height:1.5">'
            '<b>💰 수익 중심</b> (안정 제품)<br>'
            '• 핵심 KPI: <b>영업이익, ROAS, BE ACoS</b><br>'
            '• 판단: ROAS가 BE 이상? 영업이익 흑자?<br>'
            '• Verdict: ROAS ≥ BE×1.2 → SCALE | ×0.9 → HOLD | ×0.7 → FIX | 미만 → CUT<br>'
            '• 적자 캠페인 즉시 정리 대상'
            '</div>', unsafe_allow_html=True
        )

    st.markdown("**🔀 정렬 기준**")
    SORT_METRIC = st.selectbox("",["spend","sales","acos_pct","roas","orders","clicks"],
                                label_visibility="collapsed",key="g_sort")
    SORT_ASC = st.toggle("오름차순 ↑",value=False,key="g_sort_asc")

    st.divider()
    st.markdown("**🔑 최소 클릭 (검색어 필터)**")
    MIN_CLICKS = st.number_input("",value=5,min_value=1,step=1,
                                  label_visibility="collapsed",key="g_min_clicks")

    st.divider()
    st.markdown("**🏷 브랜드 키워드**")
    BRAND_KEYWORDS = st.text_input(
        "", value="neo", label_visibility="collapsed", key="g_brand_kw",
        help="브랜드 검색어 판별에 사용. 쉼표로 여러 개 입력 가능. 예: neo, neochair"
    )
    _BRAND_KW_LIST = [kw.strip().lower() for kw in BRAND_KEYWORDS.split(",") if kw.strip()]
    if _BRAND_KW_LIST:
        st.caption(f"🏷 '{', '.join(_BRAND_KW_LIST)}'  포함 검색어를 브랜드로 분류")

    st.divider()
    if HAS_COGS: st.success("✅ 원가 연결")
    else:        st.info("ℹ Proxy 모드")

# ════════════════════════════════════════════════════════════════════════
# 9. APPLY FILTERS
# ════════════════════════════════════════════════════════════════════════
_mask = (fact_merged_base["date"]>=D0) & (fact_merged_base["date"]<=D1)
if sel_type: _mask &= fact_merged_base["campaign_type"].isin(sel_type)
if sel_camp: _mask &= fact_merged_base["campaign"].isin(sel_camp)
if sel_sku:  _mask &= fact_merged_base["sku"].isin(sel_sku)
df_raw_filtered = fact_merged_base[_mask].copy()

if df_raw_filtered.empty:
    st.warning("⚠ 선택 조건에 해당하는 데이터가 없습니다.")
    st.stop()

# ════════════════════════════════════════════════════════════════════════
# 10. COST ASSUMPTIONS PANEL (접이식) + CANONICAL PROFIT LAYER
# ════════════════════════════════════════════════════════════════════════
st.markdown("---")
with st.expander("💰 비용 가정값 조정 (클릭해서 열기)", expanded=False):

    # ── 📖 비용 구조 설명 ──
    st.markdown("""
##### 📖 영업이익은 어떻게 계산되나요?

Amazon PPC 광고의 영업이익은 다음과 같이 계산됩니다:
""")
    st.code(
        "영업이익 = 판매가(Sales) − 제품원가 − 추가비용 − 광고비(Spend)", language=""
    )
    st.markdown("""
각 항목의 의미는 다음과 같습니다:

| 구분 | 항목 | 설명 | 데이터 출처 |
|------|------|------|------------|
| **판매가** | Sales | 광고를 통해 발생한 실제 매출액 | PPC 리포트 자동 반영 |
| **제품원가** | unit_cogs | FOB(공장출고가) + 물류비(관세, 해상, 로컬, DEVAN, 보관, SHIP) + 운영비 5% | 원가 마스터(unit_cogs.xlsx) |
| **추가비용** | 아래 레버로 조정 | 판매 채널(1P/3P)에 따라 Amazon이 부과하는 수수료·배송비·기타 차감항목 | 아래에서 직접 설정 |
| **광고비** | Spend | PPC 캠페인에 사용된 광고비 | PPC 리포트 자동 반영 |

> **💡 핵심**: `판매가`와 `광고비`는 PPC 데이터에서 자동으로 들어옵니다.  
> `제품원가`는 원가 파일에서 SKU별로 매칭됩니다.  
> **아래 레버들**은 원가 파일에 포함되지 않은 **추가 비용**을 설정하는 곳입니다.

---
""")

    st.markdown("""
##### ⚙️ 추가 비용 설정

아래 레버들이 **모든 탭의 영업이익·BE(손익분기) 계산에 실제 반영**됩니다.  
원가 파일(unit_cogs)에 이미 포함된 항목은 **0%/0**으로 두세요.  
포함 여부가 불확실하면 원가 담당자에게 확인 후 조정하세요.
""")

    # ── 기본 비용 ──
    st.markdown("##### 📦 기본 비용")
    st.caption("모든 판매 채널(1P/3P)에 공통으로 적용되는 비용입니다.")
    _ba_c1, _ba_c2 = st.columns(2)
    with _ba_c1:
        _cost_mult_pct = st.slider(
            "제품원가 조정 (%)",
            min_value=-30, max_value=50, value=0, step=1,
            format="%d%%", key="cost_mult",
            help="원가 마스터의 unit_cogs를 일괄 조정합니다. "
                 "예: +10% → 원가 $100인 제품이 $110으로 계산. "
                 "환율 변동, 원자재 가격 상승 등을 반영할 때 사용."
        )
    with _ba_c2:
        _return_pct = st.slider(
            "↩️ 반품률 (%)",
            min_value=0, max_value=30, value=0, step=1,
            format="%d%%", key="return_pct",
            help="반품 시 판매가의 50%를 손실로 가정합니다. "
                 "예: 반품률 10%, 판매가 $100 → 개당 $5 추가 비용. "
                 "가구/매트리스 카테고리 평균 반품률: 5~15%."
        )

    st.markdown("---")

    # ── 3P 비용 (직접 판매 시) ──
    st.markdown("##### 🛒 3P 비용 (직접 판매)")
    st.caption(
        "3P = 셀러가 직접 Amazon에서 소비자에게 판매하는 구조입니다. "
        "Amazon이 판매 수수료(Referral Fee)와 FBA 배송비를 부과합니다."
    )
    _3p_on = st.toggle("3P 비용 적용", value=False, key="toggle_3p",
                        help="3P 판매 시 ON. 1P(Amazon 도매) 전용이면 OFF로 두세요.")
    if _3p_on:
        _3p_c1, _3p_c2 = st.columns(2)
        with _3p_c1:
            _amazon_fee_pct = st.slider(
                "🏷️ Amazon 판매 수수료 (%)",
                min_value=0, max_value=25, value=15, step=1,
                format="%d%%", key="amazon_fee",
                help="Amazon Referral Fee. 판매가의 일정 %를 수수료로 차감. "
                     "카테고리별 8~15%. 가구류 보통 15%."
            )
        with _3p_c2:
            _fba_ship = st.number_input(
                "🚚 FBA 배송비 ($/unit)",
                min_value=0.0, max_value=30.0, value=11.0, step=0.5,
                key="fba_ship",
                help="FBA Fulfillment Fee. Amazon 물류센터에서 소비자까지 배송하는 비용. "
                     "제품 크기/무게에 따라 다름. 가구류 보통 $8~$15."
            )
    else:
        _amazon_fee_pct = 0
        _fba_ship = 0.0

    st.markdown("---")

    # ── 1P 비용 (도매 시) ──
    st.markdown("##### 🏢 1P 비용 (Amazon 도매 — DF 납품)")
    st.caption(
        "1P = Amazon에 도매로 납품하고, Amazon이 소비자에게 판매하는 구조입니다.  \n"
        "현재 **DF(Direct Fulfillment)** 방식 사용 — FA 수수료 미발생.  \n"
        "캠페인명이 `1P`로 시작하는 행에만 자동 적용됩니다."
    )
    _1p_on = st.toggle("1P 비용 적용", value=False, key="toggle_1p",
                        help="1P 캠페인이 포함된 데이터일 때 ON. 캠페인명 '1P'로 시작하는 행에만 적용.")
    if _1p_on:
        _1p_r0 = st.columns(2)
        with _1p_r0[0]:
            _net_ppm_pct = st.slider(
                "📊 Net PPM (%)",
                min_value=15, max_value=45, value=27, step=1,
                format="%d%%", key="net_ppm",
                help="Amazon의 목표 수익률 (Net Pure Profit Margin). "
                     "현재 27% (2026 동결). Net PPM에서 도매가 비율이 자동 계산됩니다."
            )
        with _1p_r0[1]:
            _deal_pct_input = st.slider(
                "🏷️ 1P 딜 할인율 (%)",
                min_value=0, max_value=30, value=10, step=1,
                format="%d%%", key="deal_pct",
                help="1P 딜 적용 시 할인율. 기본 10%. "
                     "딜 미적용 시 0%로 설정. 딜 OFF → 이익이 딜 funding만큼 증가."
            )
        _1p_c1, _1p_c2 = st.columns(2)
        with _1p_c1:
            _coop_pct = st.slider(
                "🤝 COOP (%)",
                min_value=0, max_value=15, value=8, step=1,
                format="%d%%", key="coop_pct",
                help="Co-Op 마케팅 협력금. **도매가** 기준 %. 보통 8%."
            )
        with _1p_c2:
            _da_pct = st.slider(
                "📦 DA — Damage Allowance (%)",
                min_value=0, max_value=10, value=3, step=1,
                format="%d%%", key="da_pct",
                help="파손/반품 충당금. **도매가** 기준 %. 보통 3%."
            )
        # FA removed from UI — DF fulfillment means no FA charge from Amazon
        _fa_pct = 0  # FA excluded under DF

        # Compute derived values
        _allowance_rate = (_coop_pct + _da_pct) / 100.0
        _net_ppm_frac = _net_ppm_pct / 100.0
        _deal_frac = _deal_pct_input / 100.0
        _wholesale_ratio = (1.0 - _net_ppm_frac) / (1.0 - _allowance_rate)
        _wholesale_pct = _wholesale_ratio * 100
        _factor_1p = (1.0 - _wholesale_ratio * (1.0 - _allowance_rate)) / max(1.0 - _deal_frac, 0.01)

        st.caption(
            f"→ 도매가 비율 = (1−{_net_ppm_pct}%) / (1−{_coop_pct+_da_pct}%) "
            f"= **{_wholesale_pct:.1f}%**  \n"
            f"→ 1P 채널비용 factor = (1−{_wholesale_pct:.1f}%×{100-_coop_pct-_da_pct}%) "
            f"/ (1−{_deal_pct_input}%) = **{_factor_1p*100:.1f}%**"
        )

        if _3p_on:
            st.info(
                "ℹ️ **1P/3P 동시 활성화** — 캠페인명이 `1P`로 시작하는 행에는 1P 비용만, "
                "나머지 행에는 3P 비용만 적용됩니다. 이중 적용 없이 행 레벨로 분리됩니다."
            )
    else:
        _coop_pct = 0
        _da_pct = 0
        _fa_pct = 0
        _net_ppm_pct = 27
        _deal_pct_input = 10
        _wholesale_pct = 82

    # ── 가정값 요약 ──
    _cost_mult = _cost_mult_pct / 100.0
    _fee_frac  = _amazon_fee_pct / 100.0
    _ret_frac  = _return_pct / 100.0
    _coop_frac = _coop_pct / 100.0
    _da_frac   = _da_pct / 100.0
    _fa_frac   = _fa_pct / 100.0
    _deal_frac = _deal_pct_input / 100.0 if _1p_on else 0.10
    _net_ppm_frac = _net_ppm_pct / 100.0 if _1p_on else 0.27
    _allowance_rate = _coop_frac + _da_frac  # FA excluded under DF
    _wholesale_ratio = (1.0 - _net_ppm_frac) / max(1.0 - _allowance_rate, 0.01)
    _wholesale_pct_calc = _wholesale_ratio * 100

    _any_extra = (_amazon_fee_pct > 0 or _fba_ship > 0 or _return_pct > 0
                  or _coop_pct > 0 or _da_pct > 0)

    st.markdown("---")
    st.markdown("##### 📐 최종 적용 공식")

    # ── 1P 공식 ──
    if _1p_on:
        _factor_display = (1.0 - _wholesale_ratio * (1.0 - _allowance_rate)) / max(1.0 - _deal_frac, 0.01)
        st.markdown(f"**🏢 1P 행** (캠페인 `1P*`)")
        st.code(
            f"Net PPM = {_net_ppm_pct}%  (Amazon 목표 수익률, 2026 동결)\n"
            f"도매가 비율 = (1−{_net_ppm_pct}%) / (1−{_coop_pct+_da_pct}%) = {_wholesale_pct_calc:.1f}%\n"
            f"Allowances(도매가 기준) = COOP {_coop_pct}% + DA {_da_pct}% = {_coop_pct+_da_pct}%  (FA 제외: DF 기준)\n"
            f"딜 할인율 = {_deal_pct_input}%\n"
            f"→ 채널비용 factor = (1 − {_wholesale_pct_calc:.1f}% × {100-_coop_pct-_da_pct}%) / (1 − {_deal_pct_input}%) = {_factor_display*100:.1f}%\n"
            f"channel_ded = avg_price × {_factor_display*100:.1f}%\n"
            f"unit_cogs_adj = 제품원가 + 채널차감({_factor_display*100:.1f}%)"
            + (f" + 반품({_return_pct}%×50%)" if _return_pct > 0 else ""), language="")

    # ── 3P 공식 ──
    if _3p_on:
        st.markdown(f"**🛒 3P 행** (캠페인 `1P*` 외)")
        _3p_parts = ["제품원가"]
        if _amazon_fee_pct > 0: _3p_parts.append(f"판매가 × {_amazon_fee_pct}% (수수료)")
        if _fba_ship > 0:       _3p_parts.append(f"${_fba_ship:.1f}/unit (FBA)")
        if _return_pct > 0:     _3p_parts.append(f"판매가 × {_return_pct}% × 50% (반품)")
        st.code(f"unit_cogs_adj = {' + '.join(_3p_parts)}", language="")

    # ── 공통 ──
    st.code("[영업이익] = Sales − Units × unit_cogs_adj − Spend(광고비)", language="")

    if not _any_extra:
        st.info(
            "ℹ️ 추가 비용 항목이 모두 0입니다.  \n"
            "원가 파일의 unit_cogs가 모든 비용을 포함한 ALL-IN 원가라면 이대로 OK.  \n"
            "포함하지 않는 비용이 있다면 위에서 해당 항목을 켜주세요."
        )
    if _1p_on:
        st.success(
            f"🏢 1P 행: 판매가의 **{_factor_display*100:.1f}%** 채널 비용 차감  "
            f"(Net PPM {_net_ppm_pct}% + 딜 {_deal_pct_input}% 보정)"
        )
    if _3p_on:
        st.success(f"🛒 3P 행: 수수료({_amazon_fee_pct}%) + FBA(${_fba_ship:.1f}) 적용")

_cost_mult = _cost_mult_pct / 100.0
_fee_frac  = _amazon_fee_pct / 100.0
_ret_frac  = _return_pct / 100.0
_coop_frac = _coop_pct / 100.0
_da_frac   = _da_pct / 100.0
_fa_frac   = _fa_pct / 100.0
_deal_frac = _deal_pct_input / 100.0 if _1p_on else 0.10
_net_ppm_frac = _net_ppm_pct / 100.0 if _1p_on else 0.27
_allowance_rate = _coop_frac + _da_frac
_wholesale_ratio = (1.0 - _net_ppm_frac) / max(1.0 - _allowance_rate, 0.01)

# Apply canonical model with all cost assumptions (row-level 1P/3P gating)
df = transform_add_profit(df_raw_filtered, _cost_mult,
                          amazon_fee_pct=_fee_frac,
                          fba_ship_per_unit=_fba_ship,
                          return_pct=_ret_frac,
                          coop_pct=_coop_frac,
                          da_pct=_da_frac,
                          fa_pct=_fa_frac,
                          wholesale_ratio=_wholesale_ratio,
                          deal_pct=_deal_frac)

if df["is_proxy_cost"].any():
    st.warning("⚠️ **Proxy Cost 적용됨** — 원가 미매핑 ASIN: `avg_price × 0.70` 임시 사용. "
               "설정 탭에서 미매핑 ASIN 확인 후 원가 마스터 보완 권고.")

with st.expander("📐 영업이익 공식 (전 탭 공통)", expanded=False):
    _exp_allow = _coop_frac + _da_frac
    _exp_factor = (1.0 - _wholesale_ratio * (1.0 - _exp_allow)) / max(1.0 - _deal_frac, 0.01)
    st.code(
        "── 공통 ──\n"
        "base_cogs   = unit_cogs × (1 + cost_mult)   [미매칭: avg_price × 0.70]\n"
        "return_cost = avg_price × return_pct × 0.50\n"
        "\n── 1P 행 (campaign '1P*', DF 납품) ──\n"
        f"Net PPM           = {_net_ppm_frac:.2f}  (Amazon 목표 수익률)\n"
        f"wholesale_ratio   = (1−{_net_ppm_frac:.2f}) / (1−{_exp_allow:.2f}) = {_wholesale_ratio:.4f}\n"
        f"allowances        = COOP {_coop_frac:.2f} + DA {_da_frac:.2f} = {_exp_allow:.2f}  (FA 제외: DF)\n"
        f"deal_pct          = {_deal_frac:.2f}\n"
        f"factor            = (1 − {_wholesale_ratio:.4f} × {1-_exp_allow:.2f}) / (1 − {_deal_frac:.2f}) = {_exp_factor:.4f}\n"
        f"channel_deduction = avg_price × {_exp_factor:.4f}  ({_exp_factor*100:.1f}%)\n"
        "unit_cogs_adj(1P) = base_cogs + channel_deduction + return_cost\n"
        "\n── 3P 행 (나머지) ──\n"
        f"channel_cost      = avg_price × {_fee_frac:.2f} + ${_fba_ship:.1f}\n"
        "unit_cogs_adj(3P) = base_cogs + channel_cost + return_cost\n"
        "\n── 최종 (전 탭 공통) ──\n"
        "operating_profit  = sales − units × unit_cogs_adj − spend\n"
        "be_spend          = sales − units × unit_cogs_adj\n"
        "be_roas           = sales / be_spend  (≤0 → ∞)\n"
        "be_acos (%)       = be_spend / sales × 100", language="")

# ── Global scalar KPIs ────────────────────────────────────────────────
_SPEND  = float(df["spend"].sum())
_SALES  = float(df["sales"].sum())
_ORDERS = float(df["orders"].sum())
_UNITS  = float(df["units"].sum())
_CLICKS = float(df["clicks"].sum())
_IMPR   = float(df["impressions"].sum())
_LTS    = float(df["long_term_sales"].sum())
_PROFIT = float(df["operating_profit"].sum())

_ROAS     = safe_div(_SALES,_SPEND)
_ACOS     = safe_div(_SPEND,_SALES,pct=True)
_CPC      = safe_div(_SPEND,_CLICKS)
_CTR      = safe_div(_CLICKS,_IMPR,pct=True)
_CVR_ORD  = safe_div(_ORDERS,_CLICKS,pct=True)
_CVR_UNIT = safe_div(_UNITS,_CLICKS,pct=True)
_AOV      = safe_div(_SALES,_ORDERS)
_AVG_PRICE= safe_div(_SALES,_UNITS)
_CPA_ORD  = safe_div(_SPEND,_ORDERS)
_CPA_UNIT = safe_div(_SPEND,_UNITS)

# ════════════════════════════════════════════════════════════════
# Total-level BE  ── CORRECT formula (v3.1 bugfix)
#
#   unit_cogs_adj  = per-unit cost  →  must multiply by units
#   total_cogs     = sum(units × unit_cogs_adj)   ← correct
#   be_spend       = sales - total_cogs           ← correct
#
#   WRONG (previous): df["unit_cogs_adj"].sum()
#     → sums per-unit values across rows = massively over-counted
#     → be_spend goes negative → BE_ROAS = 999
#
#   We also expose sum(be_spend) directly from transform_add_profit
#   which already computed  be_spend_row = sales_row - units_row*cogs_adj_row
#   Summing those gives the correct aggregate be_spend.
# ════════════════════════════════════════════════════════════════
_TOTAL_COGS_ADJ = float((df["units"] * df["unit_cogs_adj"]).sum())   # sum(units × unit_cogs_adj)
_BE_SPEND_TOT   = _SALES - _TOTAL_COGS_ADJ                           # = sum(be_spend) per row
_BE_ROAS_TOT    = (_SALES / _BE_SPEND_TOT) if _BE_SPEND_TOT > 0 else 999.
_BE_ACOS_TOT    = (_BE_SPEND_TOT / _SALES * 100) if (_BE_SPEND_TOT > 0 and _SALES > 0) else 0.

# Weighted-average unit cost  =  total_cogs / total_units  (used in simulations)
_AVG_COGS_PU    = _TOTAL_COGS_ADJ / max(_UNITS, 1)   # $ per unit, units-weighted

# ── Debug expander: BE 계산 검증 로그 ────────────────────────────────
# 캠페인 필터 변경 시 BE_ROAS/BE_ACoS가 정상 수치로 나오는지 확인용
with st.expander("🔍 BE 계산 검증 로그 (디버그)", expanded=False):
    _be_sum_row = float(df["be_spend"].sum())   # sum of row-level be_spend (should == _BE_SPEND_TOT)
    _consistency_ok = abs(_be_sum_row - _BE_SPEND_TOT) < 0.01
    st.markdown("##### Total-level BE 계산 내역")
    _dbg_rows = [
        {"항목": "Sales (합계)",              "값": fu(_SALES),         "비고": ""},
        {"항목": "Units (합계)",              "값": fi(_UNITS),         "비고": ""},
        {"항목": "Total COGS = Σ(units×cogs_adj)", "값": fu(_TOTAL_COGS_ADJ), "비고": "✅ 올바른 방식"},
        {"항목": "BE_SPEND = Sales - Total COGS",  "값": fu(_BE_SPEND_TOT),   "비고": "양수여야 정상"},
        {"항목": "Σ(be_spend per row) [검증]",    "값": fu(_be_sum_row),    "비고": "✅ 일치" if _consistency_ok else "❌ 불일치!"},
        {"항목": "BE_ROAS = Sales / BE_SPEND",     "값": fx(_BE_ROAS_TOT) if _BE_ROAS_TOT < 900 else "∞ (구조적 적자)", "비고": "정상범위: 1~10x"},
        {"항목": "BE_ACoS = BE_SPEND / Sales",     "값": fp(_BE_ACOS_TOT),   "비고": "정상범위: 10~70%"},
        {"항목": "Avg COGS/unit = Total COGS / Units", "값": fu(_AVG_COGS_PU), "비고": "units 가중평균"},
    ]
    st.dataframe(pd.DataFrame(_dbg_rows), use_container_width=True, hide_index=True)

    if _BE_ROAS_TOT >= 900:
        st.error(
            "⛔ BE_ROAS = ∞ (999) — 구조적 적자 상태입니다.  \n"
            f"BE_SPEND = {fu(_BE_SPEND_TOT)} ≤ 0 → 원가가 매출보다 큽니다.  \n"
            f"Total COGS ({fu(_TOTAL_COGS_ADJ)}) > Sales ({fu(_SALES)})  \n"
            "원인: 원가 마스터 값 확인 또는 Cost Multiplier 조정 필요."
        )
    elif not _consistency_ok:
        st.warning(f"⚠️ 행별 be_spend 합계 불일치: {fu(_be_sum_row)} vs {fu(_BE_SPEND_TOT)}")
    else:
        st.success(
            f"✅ BE 계산 정상 — BE_ROAS {fx(_BE_ROAS_TOT)} · "
            f"BE_ACoS {fp(_BE_ACOS_TOT)} · "
            f"현재 ROAS {fx(_ROAS)} → {'흑자' if _ROAS >= _BE_ROAS_TOT else '적자'}"
        )

    if HAS_COGS:
        _proxy_rows = int(df["is_proxy_cost"].sum())
        _proxy_pct  = safe_div(_proxy_rows, len(df), pct=True)
        st.caption(
            f"Proxy Cost 행: {fi(_proxy_rows)} ({_proxy_pct:.1f}%)  |  "
            f"실제 원가 기반 행: {fi(len(df)-_proxy_rows)}  |  "
            f"Cost Mult: {_cost_mult:+.0%} · Fee: {_amazon_fee_pct}% · Ship: ${_fba_ship:.1f} · Ret: {_return_pct}% · COOP: {_coop_pct}% · DA: {_da_pct}% · FA: {_fa_pct}%"
        )

# Session state
for _k,_v in [("dd_campaign",None),("dd_sku",None),("action_log",[])]:
    if _k not in st.session_state: st.session_state[_k]=_v


# ════════════════════════════════════════════════════════════════════════
# 11. TAB LAYOUT
# ════════════════════════════════════════════════════════════════════════
TAB1,TAB2,TAB3,TAB4,TAB5,TAB6,TAB7 = st.tabs([
    "📊 운영 현황",
    "💰 CEO 전략",
    "📈 Profit Simulation",
    "🏗 제품 구조",
    "⚔ 판매팀 액션 플랜",
    "🤖 ML",
    "🔧 설정",
])

# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 1 — 운영 현황                                              ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB1:
    # ── 전략 모드 표시 ──
    if IS_GROWTH_MODE:
        st.markdown(
            '<div style="background:#EFF6FF;border-left:4px solid #3B82F6;padding:12px 16px;border-radius:0 6px 6px 0;margin-bottom:12px">'
            '<b>📈 성장 중심 모드</b> — 핵심 KPI: <b>ACoS, CTR, CVR, 노출 점유율</b>  |  '
            '신제품 런칭·시장 점유율 확대 시: 적자를 감수하더라도 노출·클릭·전환 효율에 집중</div>',
            unsafe_allow_html=True
        )
    else:
        st.markdown(
            '<div style="background:#F0FDF4;border-left:4px solid #10B981;padding:12px 16px;border-radius:0 6px 6px 0;margin-bottom:12px">'
            '<b>💰 수익 중심 모드</b> — 핵심 KPI: <b>영업이익, ROAS, BE ACoS</b>  |  '
            '안정 제품: 수익성 최적화·적자 캠페인 정리에 집중</div>',
            unsafe_allow_html=True
        )

    # ── KPI 스코어카드 ─────────────────────────────────────────────
    sec("① 전체 KPI 스코어카드")
    _acos_cls = "kpi-good" if _ACOS<=TARGET_ACOS else ("kpi-warn" if _ACOS<=ACOS_WARN else "kpi-bad")
    r1=st.columns(5)
    kpi_card(r1[0],"광고비(Spend,$)",fu(_SPEND),color="blue")
    kpi_card(r1[1],"광고매출(Sales,$)",fu(_SALES),f"ROAS {fx(_ROAS)}",color="green")
    kpi_card(r1[2],"ACoS(%)",fp(_ACOS),f"목표 {TARGET_ACOS:.0f}% {'✓' if _ACOS<=TARGET_ACOS else '↑'}",
             sub_cls=_acos_cls,color="amber")
    kpi_card(r1[3],"노출",fi(_IMPR),f"CTR {fp(_CTR)}",color="slate")
    kpi_card(r1[4],"클릭",fi(_CLICKS),f"CPC {fu(_CPC)}",color="slate")
    r2=st.columns(4)
    kpi_card(r2[0],"CVR(%)",fp(_CVR_ORD),f"주문 {fi(_ORDERS)}건",color="purple")
    kpi_card(r2[1],"AOV($)",fu(_AOV),f"avg_price {fu(_AVG_PRICE)}",color="teal")
    kpi_card(r2[2],"CPA($)",fu(_CPA_ORD),color="red" if _CPA_ORD>_AOV else "blue")
    kpi_card(r2[3],"영업이익(추정,$)",fu(_PROFIT),
             "🟢 흑자" if _PROFIT>0 else "🔴 적자",
             sub_cls="kpi-good" if _PROFIT>0 else "kpi-bad",
             color="green" if _PROFIT>0 else "red")
    st.caption(f"기간: {D0.date()} ~ {D1.date()}  |  캠페인: {df['campaign'].nunique()}개  |  ASIN: {df['asin'].nunique()}개  |  모드: {PPC_MODE}")
    st.divider()

    # ── 전략 모드별 핵심 진단 ──
    if IS_GROWTH_MODE:
        sec("📈 성장 모드 — 광고 효율 진단")
        _gm_c1, _gm_c2, _gm_c3, _gm_c4 = st.columns(4)
        _gm_c1.metric(
            "ACoS vs 목표", fp(_ACOS),
            delta=f"{_ACOS - TARGET_ACOS:+.1f}%p" if _ACOS != TARGET_ACOS else "적정",
            delta_color="inverse"
        )
        _gm_c2.metric(
            "CTR (노출→클릭)", fp(_CTR),
            delta="양호" if _CTR >= 0.35 else "개선 필요",
            delta_color="normal" if _CTR >= 0.35 else "inverse"
        )
        _gm_c3.metric(
            "CVR (클릭→주문)", fp(_CVR_ORD),
            delta="양호" if _CVR_ORD >= 8 else "개선 필요",
            delta_color="normal" if _CVR_ORD >= 8 else "inverse"
        )
        _gm_c4.metric(
            "CPC (클릭당 비용)", fu(_CPC),
            delta=f"AOV의 {_CPC/_AOV*100:.1f}%" if _AOV > 0 else "—",
            delta_color="inverse"
        )
        st.markdown(f"""
**📈 성장 모드 핵심 지표 해석:**

| 지표 | 현재 | 기준 | 의미 | 진단 |
|------|------|------|------|------|
| **ACoS** | {fp(_ACOS)} | ≤ {TARGET_ACOS:.0f}% | 매출 대비 광고비 비율. **낮을수록 효율적** | {'✅ 목표 이내' if _ACOS <= TARGET_ACOS else '⚠️ 목표 초과 — Bid/타겟 최적화 필요'} |
| **CTR** | {fp(_CTR)} | ≥ 0.35% | 노출 대비 클릭률. **광고 소재·제목의 매력도** | {'✅ 양호' if _CTR >= 0.35 else '🔴 낮음 — 메인 이미지, 제목, 가격 경쟁력 점검'} |
| **CVR** | {fp(_CVR_ORD)} | ≥ 8% | 클릭 대비 구매 전환율. **리스팅 설득력** | {'✅ 양호' if _CVR_ORD >= 8 else '🔴 낮음 — 리스팅, 리뷰, 가격, A+ 콘텐츠 점검'} |
| **CPC** | {fu(_CPC)} | ≤ AOV×5% | 클릭당 단가. **입찰 경쟁 강도** | {'✅ 적정' if _AOV > 0 and _CPC/_AOV*100 <= 5 else '⚠️ 높음 — 롱테일/니치 키워드로 전환'} |

> 💡 **신제품 런칭 시 ACoS가 높은 것은 정상입니다.** 초기에는 리뷰·랭킹이 부족하여 CTR/CVR이 낮고,
> 이를 보상하기 위해 높은 Bid가 필요합니다. 핵심은 **ACoS가 시간이 지남에 따라 하락 추세를 보이느냐**입니다.
""")
        st.divider()

        # ── 성장 모드 서브탭 ──
        _gt1, _gt2, _gt3, _gt4, _gt5 = st.tabs([
            "📊 효율 스코어보드", "🏷 브랜드 인지도", "📈 주간 추이",
            "🆕 신제품 분석", "🎮 시뮬레이터"
        ])

        with _gt1:
            # TACoS (Total ACoS) = Spend / 총매출(광고+오가닉)
            _total_sales_w_organic = max(_LTS, _SALES)  # Long-term sales가 있으면 사용
            _tacos = safe_div(_SPEND, _total_sales_w_organic, pct=True)
            # Ad Efficiency Score = CTR × CVR × 1000 (높을수록 좋음)
            _ad_eff = _CTR * _CVR_ORD * 10  # 스케일 조정
            # CPA/AOV 비율 (1 미만이어야 건전)
            _cpa_aov_ratio = safe_div(_CPA_ORD, _AOV) if _AOV > 0 else 999

            _gm_s1, _gm_s2, _gm_s3, _gm_s4 = st.columns(4)
            _gm_s1.metric(
                "TACoS (%)", fp(_tacos),
                help="Total ACoS = 광고비 ÷ 총매출(광고+오가닉). 낮을수록 오가닉 매출 비중이 높음. 10% 이하가 이상적."
            )
            _gm_s2.metric(
                "Ad Efficiency", f"{_ad_eff:.1f}",
                delta="양호" if _ad_eff >= 3 else "개선 필요",
                delta_color="normal" if _ad_eff >= 3 else "inverse",
                help="CTR × CVR × 10. 광고 노출→전환까지의 통합 효율. 3 이상이면 양호."
            )
            _gm_s3.metric(
                "CPA / AOV", f"{_cpa_aov_ratio:.2f}",
                delta="건전" if _cpa_aov_ratio < 1 else "위험 — 주문당 적자",
                delta_color="normal" if _cpa_aov_ratio < 1 else "inverse",
                help="주문당 획득비용 ÷ 평균 주문가. 1 이상이면 한 건 팔 때마다 광고비가 매출 초과."
            )
            _gm_s4.metric(
                "오가닉 비율", f"{max(0, (1 - _SALES/_total_sales_w_organic))*100:.0f}%" if _total_sales_w_organic > _SALES else "0%",
                help="총매출 중 광고 외 오가닉 매출 비중. 높을수록 광고 의존도 낮음."
            )

            st.markdown("""
    | 지표 | 공식 | 의미 | 좋은 상태 | 활용법 |
    |------|------|------|---------|--------|
    | **TACoS** | 광고비 ÷ 총매출(광고+오가닉) | 전체 사업에서 광고에 얼마나 의존하는가 | ≤ 10% | 오가닉 매출↑이면 TACoS 자연 하락. 브랜드 성숙도의 장기 지표 |
    | **Ad Efficiency** | CTR × CVR × 10 | 광고 노출→클릭→구매까지의 통합 설득력 | ≥ 3.0 | 낮으면 퍼널 분해: CTR 낮으면 소재 문제, CVR 낮으면 리스팅 문제 |
    | **CPA / AOV** | 주문당 획득비용 ÷ 평균 주문가 | 한 건 팔 때 광고비가 매출의 몇 배인가 | < 1.0 | 1 이상이면 팔수록 적자. Bid↓ 또는 CVR↑로 CPA를 낮춰야 함 |
    | **오가닉 비율** | (총매출 − 광고매출) ÷ 총매출 | 광고 없이도 팔리는 매출의 비중 | ≥ 50% | 높을수록 브랜드 파워↑. 신제품은 0%→점차 상승이 정상 경로 |
    """)
            st.divider()

            # ── 🏷 브랜드 인지도 분석 ──
        with _gt2:
            sec("🏷 브랜드 인지도 — 검색어 분석")
            if _BRAND_KW_LIST:
                # 브랜드 vs 비브랜드 분류
                _brand_mask = df["search_term"].str.lower().apply(
                    lambda x: any(kw in str(x) for kw in _BRAND_KW_LIST) if pd.notna(x) else False
                )
                _df_brand = df[_brand_mask]
                _df_nonbrand = df[~_brand_mask]

                _b_spend = float(_df_brand["spend"].sum())
                _b_sales = float(_df_brand["sales"].sum())
                _b_clicks = float(_df_brand["clicks"].sum())
                _b_orders = float(_df_brand["orders"].sum())
                _b_impr = float(_df_brand["impressions"].sum())
                _nb_spend = float(_df_nonbrand["spend"].sum())
                _nb_sales = float(_df_nonbrand["sales"].sum())
                _nb_clicks = float(_df_nonbrand["clicks"].sum())
                _nb_orders = float(_df_nonbrand["orders"].sum())
                _nb_impr = float(_df_nonbrand["impressions"].sum())

                # ① 브랜드 비중 메트릭
                st.markdown("##### ① 브랜드 검색어 비중")
                _bm1, _bm2, _bm3, _bm4 = st.columns(4)
                _b_sales_pct = _b_sales / max(_SALES, 1) * 100
                _b_clicks_pct = _b_clicks / max(_CLICKS, 1) * 100
                _b_spend_pct = _b_spend / max(_SPEND, 1) * 100
                _b_orders_pct = _b_orders / max(_ORDERS, 1) * 100
                _bm1.metric("Sales 비중", f"{_b_sales_pct:.1f}%",
                             help="브랜드 검색어를 통한 매출 비중")
                _bm2.metric("Clicks 비중", f"{_b_clicks_pct:.1f}%",
                             help="브랜드 검색어의 클릭 비중")
                _bm3.metric("Spend 비중", f"{_b_spend_pct:.1f}%",
                             help="브랜드 검색어에 쓴 광고비 비중 (낮을수록 효율적)")
                _bm4.metric("Orders 비중", f"{_b_orders_pct:.1f}%",
                             help="브랜드 검색어를 통한 주문 비중")

                if _b_sales_pct < 5:
                    st.info(
                        f"🏷 브랜드 검색 비중 {_b_sales_pct:.1f}% — 대부분 비브랜드(일반) 검색어를 통해 유입됩니다.  \n"
                        "브랜드 인지도가 아직 낮은 상태로, 외부 마케팅(소셜/인플루언서)이나 "
                        "Sponsored Brands 캠페인으로 인지도를 높이면 이 비중이 올라갑니다."
                    )
                elif _b_sales_pct < 20:
                    st.success(f"🏷 브랜드 검색 비중 {_b_sales_pct:.1f}% — 브랜드 인지도가 형성되기 시작했습니다.")
                else:
                    st.success(f"🏷 브랜드 검색 비중 {_b_sales_pct:.1f}% — 브랜드 파워가 강합니다. 브랜드 방어 캠페인을 유지하세요.")
                st.divider()

                # ② 브랜드 vs 비브랜드 효율 비교
                st.markdown("##### ② 브랜드 vs 비브랜드 효율 비교")
                st.caption("브랜드 검색어는 이미 제품을 아는 고객이므로, 비브랜드보다 CVR이 높고 ACoS가 낮은 것이 정상입니다.")
                _b_acos = safe_div(_b_spend, _b_sales, pct=True)
                _nb_acos = safe_div(_nb_spend, _nb_sales, pct=True)
                _b_ctr = safe_div(_b_clicks, _b_impr, pct=True)
                _nb_ctr = safe_div(_nb_clicks, _nb_impr, pct=True)
                _b_cvr = safe_div(_b_orders, _b_clicks, pct=True)
                _nb_cvr = safe_div(_nb_orders, _nb_clicks, pct=True)
                _b_cpc = safe_div(_b_spend, _b_clicks)
                _nb_cpc = safe_div(_nb_spend, _nb_clicks)
                _b_aov = safe_div(_b_sales, _b_orders)
                _nb_aov = safe_div(_nb_sales, _nb_orders)

                _brand_comp = pd.DataFrame([
                    {"지표": "ACoS (%)", "🏷 브랜드": fp(_b_acos), "🔍 비브랜드": fp(_nb_acos),
                     "차이": f"{_b_acos - _nb_acos:+.1f}%p", "해석": "✅ 브랜드가 효율적" if _b_acos < _nb_acos else "⚠️ 비브랜드가 효율적"},
                    {"지표": "CTR (%)", "🏷 브랜드": fp(_b_ctr), "🔍 비브랜드": fp(_nb_ctr),
                     "차이": f"{_b_ctr - _nb_ctr:+.2f}%p", "해석": "✅ 브랜드 클릭률 우수" if _b_ctr > _nb_ctr else "—"},
                    {"지표": "CVR (%)", "🏷 브랜드": fp(_b_cvr), "🔍 비브랜드": fp(_nb_cvr),
                     "차이": f"{_b_cvr - _nb_cvr:+.1f}%p", "해석": "✅ 브랜드 전환 우수" if _b_cvr > _nb_cvr else "—"},
                    {"지표": "CPC ($)", "🏷 브랜드": fu(_b_cpc), "🔍 비브랜드": fu(_nb_cpc),
                     "차이": f"${_b_cpc - _nb_cpc:+.2f}", "해석": "✅ 브랜드가 저렴" if _b_cpc < _nb_cpc else "⚠️ 브랜드가 비쌈"},
                    {"지표": "AOV ($)", "🏷 브랜드": fu(_b_aov), "🔍 비브랜드": fu(_nb_aov),
                     "차이": f"${_b_aov - _nb_aov:+.1f}", "해석": ""},
                    {"지표": "Spend ($)", "🏷 브랜드": fu(_b_spend), "🔍 비브랜드": fu(_nb_spend),
                     "차이": "", "해석": ""},
                    {"지표": "Sales ($)", "🏷 브랜드": fu(_b_sales), "🔍 비브랜드": fu(_nb_sales),
                     "차이": "", "해석": ""},
                ])
                st.dataframe(_brand_comp, use_container_width=True, hide_index=True)
                st.divider()

                # ③ 주간 브랜드 검색 비중 추이
                st.markdown("##### ③ 주간 브랜드 검색 비중 추이")
                st.caption("이 비중이 **올라가면** 브랜드 인지도가 성장하고 있다는 신호입니다.")
                _wk_brand = df.copy()
                _wk_brand["is_brand"] = _brand_mask.values
                _wk_brand["week"] = _wk_brand["date"].dt.to_period("W").apply(lambda p: p.start_time)
                _wk_b_agg = _wk_brand.groupby(["week", "is_brand"]).agg(
                    clicks=("clicks", "sum"), sales=("sales", "sum"), orders=("orders", "sum")
                ).reset_index()
                _wk_total = _wk_brand.groupby("week").agg(
                    total_clicks=("clicks", "sum"), total_sales=("sales", "sum")
                ).reset_index()
                _wk_b_only = _wk_b_agg[_wk_b_agg["is_brand"]].merge(_wk_total, on="week")
                _wk_b_only["brand_click_pct"] = _wk_b_only["clicks"] / _wk_b_only["total_clicks"].replace(0, np.nan) * 100
                _wk_b_only["brand_sales_pct"] = _wk_b_only["sales"] / _wk_b_only["total_sales"].replace(0, np.nan) * 100
                _wk_b_only["week_str"] = _wk_b_only["week"].dt.strftime("%m/%d")

                if len(_wk_b_only) >= 2:
                    _fig_brand = go.Figure()
                    _fig_brand.add_trace(go.Scatter(
                        x=_wk_b_only["week_str"], y=_wk_b_only["brand_sales_pct"],
                        mode="lines+markers+text", name="Sales 비중(%)",
                        text=_wk_b_only["brand_sales_pct"].map(lambda v: f"{v:.1f}%"),
                        textposition="top center",
                        line=dict(color="#8B5CF6", width=2), marker=dict(size=8)
                    ))
                    _fig_brand.add_trace(go.Scatter(
                        x=_wk_b_only["week_str"], y=_wk_b_only["brand_click_pct"],
                        mode="lines+markers", name="Clicks 비중(%)",
                        line=dict(color="#3B82F6", width=2, dash="dot"), marker=dict(size=6)
                    ))
                    _fig_brand.update_layout(
                        **{**BASE_LAYOUT, "height": 300, "title": "주간 브랜드 검색어 비중 추이",
                           "yaxis_title": "비중 (%)", "legend": dict(orientation="h", y=-0.15)}
                    )
                    st.plotly_chart(_fig_brand, use_container_width=True)

                    # 추세 판단
                    _first_b = _wk_b_only.iloc[0]["brand_sales_pct"]
                    _last_b = _wk_b_only.iloc[-1]["brand_sales_pct"]
                    _b_trend = _last_b - _first_b
                    if _b_trend > 0.5:
                        st.success(f"✅ 브랜드 비중 상승 추세 ({_first_b:.1f}% → {_last_b:.1f}%, {_b_trend:+.1f}%p) — 인지도가 성장하고 있습니다.")
                    elif _b_trend < -0.5:
                        st.warning(f"⚠️ 브랜드 비중 하락 추세 ({_first_b:.1f}% → {_last_b:.1f}%, {_b_trend:+.1f}%p) — 브랜드 캠페인 강화를 검토하세요.")
                    else:
                        st.info(f"📊 브랜드 비중 안정 ({_first_b:.1f}% → {_last_b:.1f}%, {_b_trend:+.1f}%p)")
                else:
                    st.info("주간 추이 분석에는 2주 이상의 데이터가 필요합니다.")

                # 브랜드 검색어 Top 리스트
                with st.expander("📋 브랜드 검색어 Top 20 (클릭해서 열기)", expanded=False):
                    _b_terms = agg_kpis(df[_brand_mask], ["search_term"])
                    _b_terms_top = _b_terms.sort_values("sales", ascending=False).head(20)
                    _b_terms_d = _b_terms_top[["search_term", "clicks", "orders", "spend", "sales", "acos_pct", "cvr_pct"]].copy()
                    for c in ["spend", "sales"]: _b_terms_d[c] = _b_terms_d[c].map(fu)
                    _b_terms_d["clicks"] = _b_terms_d["clicks"].map(fi)
                    _b_terms_d["orders"] = _b_terms_d["orders"].map(fi)
                    for c in ["acos_pct", "cvr_pct"]: _b_terms_d[c] = _b_terms_d[c].map(lambda v: fp(v) if pd.notna(v) else "—")
                    st.dataframe(_b_terms_d.rename(columns={
                        "search_term": "검색어", "clicks": "클릭", "orders": "주문",
                        "spend": "Spend", "sales": "Sales", "acos_pct": "ACoS(%)", "cvr_pct": "CVR(%)"
                    }), use_container_width=True, hide_index=True)
            else:
                st.info("사이드바에서 브랜드 키워드를 설정하면 브랜드 인지도 분석이 표시됩니다.")
            st.divider()

            # ── 📈 주간 ACoS/CTR/CVR 추이 (신제품 핵심: 개선 추세인가?) ──
        with _gt3:
            sec("📈 주간 광고 효율 추이 — 개선되고 있는가?")
            st.caption("신제품 핵심: 절대값보다 **추세**가 중요합니다. ACoS↓ CTR↑ CVR↑ 추세면 건강한 성장.")

            _weekly = df.copy()
            _weekly["week"] = _weekly["date"].dt.to_period("W").apply(lambda p: p.start_time)
            _wk_agg = _weekly.groupby("week").agg(
                spend=("spend","sum"), sales=("sales","sum"),
                clicks=("clicks","sum"), impressions=("impressions","sum"),
                orders=("orders","sum")
            ).reset_index()
            _wk_agg["acos_pct"] = _wk_agg["spend"] / _wk_agg["sales"].replace(0, np.nan) * 100
            _wk_agg["ctr_pct"]  = _wk_agg["clicks"] / _wk_agg["impressions"].replace(0, np.nan) * 100
            _wk_agg["cvr_pct"]  = _wk_agg["orders"] / _wk_agg["clicks"].replace(0, np.nan) * 100
            _wk_agg["cpc"]      = _wk_agg["spend"] / _wk_agg["clicks"].replace(0, np.nan)
            _wk_agg["week_str"] = _wk_agg["week"].dt.strftime("%m/%d")

            if len(_wk_agg) >= 2:
                from plotly.subplots import make_subplots as _ms
                _fig_wk = _ms(rows=2, cols=2, subplot_titles=["ACoS (%) — 낮을수록 ✓", "CTR (%) — 높을수록 ✓",
                                                                "CVR (%) — 높을수록 ✓", "CPC ($) — 낮을수록 ✓"])
                for _r, _c, _col, _nm, _clr, _good_dir in [
                    (1,1,"acos_pct","ACoS","#EF4444","down"),
                    (1,2,"ctr_pct","CTR","#3B82F6","up"),
                    (2,1,"cvr_pct","CVR","#8B5CF6","up"),
                    (2,2,"cpc","CPC","#F59E0B","down"),
                ]:
                    _vals = _wk_agg[_col].dropna()
                    # 추세선 방향 표시
                    if len(_vals) >= 2:
                        _trend = "↓" if _vals.iloc[-1] < _vals.iloc[0] else "↑"
                        _trend_good = (_good_dir == "down" and _trend == "↓") or (_good_dir == "up" and _trend == "↑")
                        _trend_icon = "✅" if _trend_good else "⚠️"
                    else:
                        _trend_icon = ""
                    _fig_wk.add_trace(go.Scatter(
                        x=_wk_agg["week_str"], y=_wk_agg[_col],
                        mode="lines+markers", name=f"{_nm} {_trend_icon}",
                        line=dict(color=_clr, width=2),
                        marker=dict(size=6)
                    ), row=_r, col=_c)
                    if _col == "acos_pct":
                        _fig_wk.add_hline(y=TARGET_ACOS, line_dash="dash", line_color="#9CA3AF",
                                          annotation_text=f"목표 {TARGET_ACOS:.0f}%", row=_r, col=_c)
                _fig_wk.update_layout(height=420, showlegend=False, margin=dict(l=40, r=20, t=40, b=30))
                st.plotly_chart(_fig_wk, use_container_width=True)

                # 추세 요약
                _first_w = _wk_agg.iloc[0]; _last_w = _wk_agg.iloc[-1]
                _acos_chg = _last_w["acos_pct"] - _first_w["acos_pct"] if pd.notna(_last_w["acos_pct"]) and pd.notna(_first_w["acos_pct"]) else 0
                _ctr_chg = _last_w["ctr_pct"] - _first_w["ctr_pct"] if pd.notna(_last_w["ctr_pct"]) and pd.notna(_first_w["ctr_pct"]) else 0
                _cvr_chg = _last_w["cvr_pct"] - _first_w["cvr_pct"] if pd.notna(_last_w["cvr_pct"]) and pd.notna(_first_w["cvr_pct"]) else 0
                _improving = (_acos_chg < 0) and (_ctr_chg > 0 or _cvr_chg > 0)

                if _improving:
                    st.success(
                        f"✅ **개선 추세** — ACoS {_acos_chg:+.1f}%p · CTR {_ctr_chg:+.2f}%p · CVR {_cvr_chg:+.1f}%p  \n"
                        f"광고 효율이 개선되고 있습니다. 현재 전략을 유지하면서 예산 확대를 검토하세요."
                    )
                elif _acos_chg > 0:
                    st.warning(
                        f"⚠️ **악화 추세** — ACoS {_acos_chg:+.1f}%p · CTR {_ctr_chg:+.2f}%p · CVR {_cvr_chg:+.1f}%p  \n"
                        f"효율이 하락하고 있습니다. 비효율 키워드 정리, Bid 조정, 리스팅 최적화를 점검하세요."
                    )
                else:
                    st.info(f"📊 ACoS {_acos_chg:+.1f}%p · CTR {_ctr_chg:+.2f}%p · CVR {_cvr_chg:+.1f}%p — 추세 판단을 위해 더 많은 데이터가 필요합니다.")
            else:
                st.info("주간 추이 분석에는 2주 이상의 데이터가 필요합니다.")
            st.divider()

            # ── 📋 캠페인별 광고 효율 랭킹 ──
            sec("📋 캠페인별 광고 효율 랭킹")
            st.caption("ACoS가 낮고, CTR·CVR이 높은 캠페인이 상위. 신제품은 이 순위를 기준으로 예산을 배분하세요.")
            _eff_camp = agg_kpis(df, ["campaign", "campaign_type"])
            _eff_camp["ad_efficiency"] = (_eff_camp["ctr_pct"] * _eff_camp["cvr_pct"] * 10).round(1)
            _eff_camp["cpa_aov"] = (_eff_camp["spend"] / _eff_camp["orders"].replace(0, np.nan) /
                                    (_eff_camp["sales"] / _eff_camp["orders"].replace(0, np.nan))).round(2)
            _eff_sort = _eff_camp.sort_values("acos_pct", ascending=True)
            _eff_cols = ["campaign", "campaign_type", "spend", "sales", "acos_pct", "ctr_pct", "cvr_pct", "cpc", "ad_efficiency", "cpa_aov", "orders"]
            _eff_cols = [c for c in _eff_cols if c in _eff_sort.columns]
            _eff_d = _eff_sort[_eff_cols].copy()
            for c in ["spend", "sales"]: _eff_d[c] = _eff_d[c].map(fu)
            for c in ["acos_pct", "ctr_pct", "cvr_pct"]: _eff_d[c] = _eff_d[c].map(lambda v: fp(v) if pd.notna(v) else "—")
            _eff_d["cpc"] = _eff_d["cpc"].map(lambda v: fu(v) if pd.notna(v) else "—")
            _eff_d["orders"] = _eff_d["orders"].map(fi)
            st.dataframe(_eff_d.rename(columns={
                "campaign": "캠페인", "campaign_type": "유형", "spend": "Spend",
                "sales": "Sales", "acos_pct": "ACoS(%)", "ctr_pct": "CTR(%)",
                "cvr_pct": "CVR(%)", "cpc": "CPC($)", "ad_efficiency": "효율점수",
                "cpa_aov": "CPA/AOV", "orders": "주문"
            }), use_container_width=True, hide_index=True, height=360)
            st.divider()

            # ── 🎮 광고 효율 개선 시뮬레이터 ──
        with _gt5:
            sec("🎮 광고 효율 개선 시뮬레이터")
            with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
                st.markdown("""
**목적:** CTR, CVR, CPC를 각각 개선했을 때 ACoS와 CPA가 어떻게 변하는지 시뮬레이션

**활용 예시:**
- "리스팅 최적화로 CVR을 +30% 올리면 ACoS가 얼마나 내려가는가?"
- "Bid를 10% 내리면(CPC -10%) 주문이 얼마나 줄어드는가?"
- "메인 이미지 교체로 CTR +20%이면 매출이 얼마나 늘어나는가?"

**핵심 인사이트:** 대부분의 경우 CVR 개선이 ACoS 개선에 가장 큰 영향. CVR은 리스팅·리뷰·A+ 콘텐츠 개선으로 올릴 수 있음.
""")
            st.caption(
                "CTR·CVR·CPC를 각각 얼마나 개선하면 ACoS와 CPA가 어떻게 변하는지 시뮬레이션합니다.  \n"
                "예: '리스팅 최적화로 CVR을 +30% 올리면 ACoS가 얼마나 내려가는가?'"
            )

            _sim_c1, _sim_c2, _sim_c3 = st.columns(3)
            with _sim_c1:
                _sim_ctr_chg = st.slider(
                    "📸 CTR 개선 (%)", -30, 100, 0, 5, key="sim_ctr_growth",
                    help="메인 이미지, 제목, 가격 경쟁력 개선으로 CTR 변화"
                )
            with _sim_c2:
                _sim_cvr_chg = st.slider(
                    "📝 CVR 개선 (%)", -30, 100, 0, 5, key="sim_cvr_growth",
                    help="리스팅, 리뷰, A+ 콘텐츠, 가격 최적화로 CVR 변화"
                )
            with _sim_c3:
                _sim_cpc_chg = st.slider(
                    "💲 CPC 변화 (%)", -50, 50, 0, 5, key="sim_cpc_growth",
                    help="Bid 조정, 키워드 최적화로 CPC 변화 (마이너스 = 절감)"
                )

            # 시뮬레이션 계산
            _sim_ctr_new  = _CTR * (1 + _sim_ctr_chg / 100)
            _sim_cvr_new  = _CVR_ORD * (1 + _sim_cvr_chg / 100)
            _sim_cpc_new  = _CPC * (1 + _sim_cpc_chg / 100)

            # 노출 고정 → 클릭 = 노출 × CTR → 주문 = 클릭 × CVR → Spend = 클릭 × CPC
            _sim_clicks  = _IMPR * (_sim_ctr_new / 100)
            _sim_orders  = _sim_clicks * (_sim_cvr_new / 100)
            _sim_spend   = _sim_clicks * _sim_cpc_new
            _sim_sales   = _sim_orders * _AOV if _AOV > 0 else 0
            _sim_acos    = (_sim_spend / _sim_sales * 100) if _sim_sales > 0 else 999
            _sim_cpa     = (_sim_spend / _sim_orders) if _sim_orders > 0 else 999
            _sim_cpa_aov = (_sim_cpa / _AOV) if _AOV > 0 else 999
            _sim_ad_eff  = _sim_ctr_new * _sim_cvr_new * 10

            # 결과 표시
            st.markdown("##### 📊 시뮬레이션 결과")
            _sr1, _sr2, _sr3, _sr4 = st.columns(4)
            _sr1.metric(
                "ACoS (%)", f"{_sim_acos:.1f}%",
                delta=f"{_sim_acos - _ACOS:+.1f}%p",
                delta_color="inverse"
            )
            _sr2.metric(
                "CPA ($)", fu(_sim_cpa),
                delta=f"{_sim_cpa - _CPA_ORD:+.1f}",
                delta_color="inverse"
            )
            _sr3.metric(
                "Ad Efficiency", f"{_sim_ad_eff:.1f}",
                delta=f"{_sim_ad_eff - _ad_eff:+.1f}",
                delta_color="normal"
            )
            _sr4.metric(
                "예상 주문", fi(int(_sim_orders)),
                delta=f"{_sim_orders - _ORDERS:+.0f}건",
                delta_color="normal"
            )

            # Before → After 비교 테이블
            _sim_compare = pd.DataFrame([
                {"지표": "CTR (%)", "현재": fp(_CTR), "시뮬": f"{_sim_ctr_new:.2f}%", "변화": f"{_sim_ctr_chg:+d}%"},
                {"지표": "CVR (%)", "현재": fp(_CVR_ORD), "시뮬": f"{_sim_cvr_new:.1f}%", "변화": f"{_sim_cvr_chg:+d}%"},
                {"지표": "CPC ($)", "현재": fu(_CPC), "시뮬": fu(_sim_cpc_new), "변화": f"{_sim_cpc_chg:+d}%"},
                {"지표": "ACoS (%)", "현재": fp(_ACOS), "시뮬": f"{_sim_acos:.1f}%", "변화": f"{_sim_acos - _ACOS:+.1f}%p"},
                {"지표": "CPA ($)", "현재": fu(_CPA_ORD), "시뮬": fu(_sim_cpa), "변화": f"{_sim_cpa - _CPA_ORD:+.1f}"},
                {"지표": "Spend ($)", "현재": fu(_SPEND), "시뮬": fu(_sim_spend), "변화": f"{(_sim_spend-_SPEND)/_SPEND*100:+.0f}%" if _SPEND>0 else "—"},
                {"지표": "Sales ($)", "현재": fu(_SALES), "시뮬": fu(_sim_sales), "변화": f"{(_sim_sales-_SALES)/_SALES*100:+.0f}%" if _SALES>0 else "—"},
                {"지표": "주문 (건)", "현재": fi(_ORDERS), "시뮬": fi(int(_sim_orders)), "변화": f"{(_sim_orders-_ORDERS)/_ORDERS*100:+.0f}%" if _ORDERS>0 else "—"},
            ])
            st.dataframe(_sim_compare, use_container_width=True, hide_index=True)

            # 핵심 인사이트
            _best_lever = "CVR" if abs(_sim_cvr_chg) >= abs(_sim_ctr_chg) and abs(_sim_cvr_chg) >= abs(_sim_cpc_chg) else \
                          "CTR" if abs(_sim_ctr_chg) >= abs(_sim_cpc_chg) else "CPC"
            if _sim_acos < _ACOS:
                st.success(
                    f"✅ ACoS {_ACOS:.1f}% → {_sim_acos:.1f}% ({_sim_acos - _ACOS:+.1f}%p 개선)  \n"
                    f"{'🎯 목표 ACoS 달성!' if _sim_acos <= TARGET_ACOS else f'목표({TARGET_ACOS:.0f}%)까지 {_sim_acos - TARGET_ACOS:.1f}%p 남음'}"
                )
            elif _sim_ctr_chg == 0 and _sim_cvr_chg == 0 and _sim_cpc_chg == 0:
                st.info("👆 위 슬라이더를 조정하여 개선 시나리오를 시뮬레이션하세요.")
            else:
                st.warning(f"⚠️ 현재 설정으로는 ACoS가 {_sim_acos - _ACOS:+.1f}%p 악화됩니다.")

            st.caption(
                "💡 **활용 팁**: CVR +30% 개선(리스팅 최적화)이 CPC -20% 절감(Bid 인하)보다 "
                "ACoS 개선 효과가 큰 경우가 많습니다. 여러 조합을 시도해보세요."
            )
            st.divider()

            # ── 🆕 신제품 분석 ──
        with _gt4:
            sec("🆕 신제품 분석")

            # 신제품 기준: 광고 활동 일수로 판단
            _asin_activity = df.groupby("asin").agg(
                first_date=("date", "min"), last_date=("date", "max"),
                active_days=("date", "nunique"),
                spend=("spend", "sum"), sales=("sales", "sum"),
                clicks=("clicks", "sum"), impressions=("impressions", "sum"),
                orders=("orders", "sum"), units=("units", "sum")
            ).reset_index()
            _data_span = (df["date"].max() - df["date"].min()).days + 1

            # 신제품 기준일 (데이터 전체 기간의 절반 미만 활동)
            _new_threshold = max(int(_data_span * 0.5), 7)
            _asin_activity["is_new"] = _asin_activity["active_days"] <= _new_threshold
            _new_asins = _asin_activity[_asin_activity["is_new"]].sort_values("first_date", ascending=False)
            _old_asins = _asin_activity[~_asin_activity["is_new"]]

            st.caption(f"신제품 기준: 광고 활동 {_new_threshold}일 이하 (전체 데이터 {_data_span}일의 50%)")

            if not _new_asins.empty:
                # ① 신제품 성적표
                st.markdown(f"##### ① 신제품 성적표 ({len(_new_asins)}개 ASIN)")
                _np_d = _new_asins.copy()
                _np_d["acos_pct"] = _np_d["spend"] / _np_d["sales"].replace(0, np.nan) * 100
                _np_d["ctr_pct"]  = _np_d["clicks"] / _np_d["impressions"].replace(0, np.nan) * 100
                _np_d["cvr_pct"]  = _np_d["orders"] / _np_d["clicks"].replace(0, np.nan) * 100
                _np_d["cpc"]      = _np_d["spend"] / _np_d["clicks"].replace(0, np.nan)
                _np_d["daily_orders"] = (_np_d["orders"] / _np_d["active_days"].replace(0, 1)).round(1)
                _np_d["daily_spend"]  = (_np_d["spend"] / _np_d["active_days"].replace(0, 1)).round(1)

                # 런칭 단계 자동 판단
                def _launch_stage(row):
                    days = row["active_days"]
                    cvr = row.get("cvr_pct", 0)
                    orders = row["orders"]
                    if days <= 7:
                        return "🟣 도입기 (1주 이내)"
                    elif cvr < 5 or orders < 5:
                        return "🟠 초기 성장기"
                    elif row.get("acos_pct", 999) > TARGET_ACOS * 2:
                        return "🟡 성장기 (ACoS 높음)"
                    elif row.get("acos_pct", 999) <= TARGET_ACOS:
                        return "🟢 안정기"
                    else:
                        return "🔵 성장기"
                _np_d["런칭 단계"] = _np_d.apply(_launch_stage, axis=1)

                _np_show = _np_d[["asin", "first_date", "active_days", "런칭 단계", "spend", "sales",
                                  "orders", "daily_orders", "acos_pct", "ctr_pct", "cvr_pct", "cpc"]].copy()
                _np_show["first_date"] = _np_show["first_date"].dt.strftime("%m/%d")
                for c in ["spend", "sales"]: _np_show[c] = _np_show[c].map(fu)
                _np_show["orders"] = _np_show["orders"].map(fi)
                for c in ["acos_pct", "ctr_pct", "cvr_pct"]:
                    _np_show[c] = _np_show[c].map(lambda v: fp(v) if pd.notna(v) else "—")
                _np_show["cpc"] = _np_show["cpc"].map(lambda v: fu(v) if pd.notna(v) else "—")

                st.dataframe(_np_show.rename(columns={
                    "asin": "ASIN", "first_date": "첫 광고일", "active_days": "활동일",
                    "spend": "Spend", "sales": "Sales", "orders": "주문",
                    "daily_orders": "일평균주문", "acos_pct": "ACoS(%)",
                    "ctr_pct": "CTR(%)", "cvr_pct": "CVR(%)", "cpc": "CPC($)"
                }), use_container_width=True, hide_index=True)

                # 런칭 단계 설명
                with st.expander("📖 런칭 단계 기준 설명", expanded=False):
                    st.markdown("""
    | 단계 | 기준 | 특징 | 전략 |
    |------|------|------|------|
    | 🟣 도입기 | 활동 7일 이내 | 데이터 부족, 판단 유보 | 노출 확보 최우선. Bid 공격적으로 |
    | 🟠 초기 성장기 | CVR < 5% 또는 주문 < 5건 | 전환이 아직 안정화 안됨 | 리스팅·리뷰·A+ 콘텐츠 보강 |
    | 🟡 성장기 (ACoS 높음) | ACoS > 목표×2 | 클릭은 오지만 비용 과다 | 비효율 키워드 정리, Bid 최적화 |
    | 🔵 성장기 | ACoS > 목표 | 효율이 점점 개선 중 | 현행 유지, 주간 추이 모니터링 |
    | 🟢 안정기 | ACoS ≤ 목표 | 광고 효율 안정 | 예산 확대 검토, 수익 모드 전환 |
    """)
                st.divider()

                # ② 신제품별 일별 ACoS 추이
                st.markdown("##### ② 신제품별 일별 ACoS 추이")
                _top_new = _new_asins.nlargest(5, "spend")["asin"].tolist()
                if _top_new:
                    _np_daily = df[df["asin"].isin(_top_new)].groupby(["date", "asin"]).agg(
                        spend=("spend", "sum"), sales=("sales", "sum")
                    ).reset_index()
                    _np_daily["acos"] = _np_daily["spend"] / _np_daily["sales"].replace(0, np.nan) * 100

                    _fig_np = go.Figure()
                    _colors_np = ["#EF4444", "#3B82F6", "#10B981", "#F59E0B", "#8B5CF6"]
                    for i, asin in enumerate(_top_new):
                        _ad = _np_daily[_np_daily["asin"] == asin]
                        _fig_np.add_trace(go.Scatter(
                            x=_ad["date"], y=_ad["acos"],
                            mode="lines+markers", name=asin[-6:],
                            line=dict(color=_colors_np[i % len(_colors_np)], width=2),
                            marker=dict(size=5)
                        ))
                    _fig_np.add_hline(y=TARGET_ACOS, line_dash="dash", line_color="#9CA3AF",
                                      annotation_text=f"목표 {TARGET_ACOS:.0f}%")
                    _fig_np.update_layout(**{**BASE_LAYOUT, "height": 320, "title": "신제품 ACoS 일별 추이 (Top 5 by Spend)",
                                             "yaxis_title": "ACoS (%)"})
                    st.plotly_chart(_fig_np, use_container_width=True)
                st.divider()
            else:
                st.info("현재 기간에 신제품으로 분류된 ASIN이 없습니다.")
                st.divider()

            # ── 🚀 신제품 런칭 시뮬레이터 ──
            sec("🚀 신제품 런칭 시뮬레이터")
            with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
                st.markdown("""
**목적:** 아직 출시하지 않은 신제품의 예상 광고 성과를 미리 시뮬레이션

**입력값 가이드:**
- **판매가:** 실제 판매 예정 가격
- **월 예산:** 런칭 초기 광고 예산 (보통 기존 제품보다 높게 시작)
- **예상 CTR:** 기존 평균의 70% 정도 (신제품은 리뷰·랭킹 없어 CTR 낮음)
- **예상 CVR:** 기존 평균의 60% 정도 (리스팅 신뢰도 부족으로 CVR 낮음)
- **예상 CPC:** 기존 평균의 130% 정도 (노출 확보 위해 높은 Bid 필요)

**핵심 판단:** 적자여도 "몇 개월 후 흑자 전환"이 가능한지가 중요. CVR이 매월 20%씩 개선되는 것이 건강한 런칭 경로.
""")
            st.caption(
                "아직 출시하지 않은 신제품도 가정값을 입력하면 예상 성과를 시뮬레이션할 수 있습니다.  \n"
                "기존 제품 평균값이 기본으로 채워져 있으며, 신제품은 보통 CTR/CVR이 낮고 CPC가 높습니다."
            )

            # 기존 제품 평균값을 기본값으로
            _default_price = round(_AOV, 0) if _AOV > 0 else 100
            _default_ctr = round(_CTR, 2) if _CTR > 0 else 0.5
            _default_cvr = round(_CVR_ORD, 1) if _CVR_ORD > 0 else 8.0
            _default_cpc = round(_CPC, 2) if _CPC > 0 else 1.50

            _lc1, _lc2 = st.columns(2)
            with _lc1:
                st.markdown("**📦 제품 정보**")
                _lnch_price = st.number_input("판매가 ($)", value=_default_price, min_value=10.0, max_value=1000.0, step=5.0, key="lnch_price")
                _lnch_budget = st.number_input("월 광고 예산 ($)", value=2000.0, min_value=100.0, max_value=50000.0, step=100.0, key="lnch_budget")
                _lnch_cogs = st.number_input(
                    "단위 원가 ($)", value=round(_AVG_COGS_PU, 0) if _AVG_COGS_PU > 0 else round(_default_price * 0.5, 0),
                    min_value=1.0, max_value=500.0, step=1.0, key="lnch_cogs",
                    help="제품원가 + 추가비용(수수료, 배송 등) 포함한 올인 원가"
                )
            with _lc2:
                st.markdown("**📊 예상 광고 지표 (신제품 가정)**")
                _lnch_ctr = st.number_input("예상 CTR (%)", value=max(_default_ctr * 0.7, 0.2), min_value=0.1, max_value=10.0, step=0.1, key="lnch_ctr",
                                             help=f"기존 평균 {_default_ctr:.2f}%. 신제품은 보통 20~30% 낮음")
                _lnch_cvr = st.number_input("예상 CVR (%)", value=max(_default_cvr * 0.6, 2.0), min_value=0.5, max_value=30.0, step=0.5, key="lnch_cvr",
                                             help=f"기존 평균 {_default_cvr:.1f}%. 신제품(리뷰 부족)은 보통 40~50% 낮음")
                _lnch_cpc = st.number_input("예상 CPC ($)", value=round(_default_cpc * 1.3, 2), min_value=0.10, max_value=10.0, step=0.05, key="lnch_cpc",
                                             help=f"기존 평균 {fu(_default_cpc)}. 신제품은 보통 20~30% 높음")

            # 시뮬레이션 계산
            _lnch_daily_budget = _lnch_budget / 30
            _lnch_daily_clicks = _lnch_daily_budget / _lnch_cpc if _lnch_cpc > 0 else 0
            _lnch_daily_impr   = _lnch_daily_clicks / (_lnch_ctr / 100) if _lnch_ctr > 0 else 0
            _lnch_daily_orders = _lnch_daily_clicks * (_lnch_cvr / 100)
            _lnch_monthly_orders = _lnch_daily_orders * 30
            _lnch_monthly_sales  = _lnch_monthly_orders * _lnch_price
            _lnch_acos = (_lnch_budget / _lnch_monthly_sales * 100) if _lnch_monthly_sales > 0 else 999
            _lnch_cpa  = (_lnch_budget / _lnch_monthly_orders) if _lnch_monthly_orders > 0 else 999
            _lnch_total_cogs = _lnch_monthly_orders * _lnch_cogs
            _lnch_profit = _lnch_monthly_sales - _lnch_total_cogs - _lnch_budget
            _lnch_margin_pct = (_lnch_profit / _lnch_monthly_sales * 100) if _lnch_monthly_sales > 0 else 0

            # 결과 표시
            st.markdown("##### 📊 월간 예상 성과")
            _lr1, _lr2, _lr3, _lr4, _lr5 = st.columns(5)
            _lr1.metric("월 주문", f"{_lnch_monthly_orders:.0f}건", f"일 {_lnch_daily_orders:.1f}건")
            _lr2.metric("월 매출", fu(_lnch_monthly_sales))
            _lr3.metric("ACoS", f"{_lnch_acos:.1f}%",
                         "✅ 목표 이내" if _lnch_acos <= TARGET_ACOS else f"목표 {TARGET_ACOS:.0f}% 초과",
                         delta_color="normal" if _lnch_acos <= TARGET_ACOS else "inverse")
            _lr4.metric("CPA", fu(_lnch_cpa), f"AOV의 {_lnch_cpa/_lnch_price*100:.0f}%" if _lnch_price > 0 else "")
            _lr5.metric("영업이익", fu(_lnch_profit),
                         f"마진 {_lnch_margin_pct:.0f}%" if _lnch_monthly_sales > 0 else "",
                         delta_color="normal" if _lnch_profit >= 0 else "inverse")

            # 손익 분해
            st.markdown("##### 💰 월간 손익 분해")
            _lnch_breakdown = pd.DataFrame([
                {"항목": "① 광고 매출", "금액": fu(_lnch_monthly_sales), "비중": "100%"},
                {"항목": "② 제품원가", "금액": f"-{fu(_lnch_total_cogs)}", "비중": f"{_lnch_total_cogs/_lnch_monthly_sales*100:.0f}%" if _lnch_monthly_sales > 0 else "—"},
                {"항목": "③ 광고비", "금액": f"-{fu(_lnch_budget)}", "비중": f"{_lnch_acos:.0f}%"},
                {"항목": "= 영업이익", "금액": fu(_lnch_profit), "비중": f"{_lnch_margin_pct:.0f}%"},
            ])
            st.dataframe(_lnch_breakdown, use_container_width=True, hide_index=True)

            # 퍼널 분해
            st.markdown("##### 📈 일일 광고 퍼널")
            _lnch_funnel = pd.DataFrame([
                {"단계": "예산", "값": f"${_lnch_daily_budget:.0f}/일"},
                {"단계": "→ 노출", "값": f"{_lnch_daily_impr:,.0f}회"},
                {"단계": f"→ 클릭 (CTR {_lnch_ctr:.1f}%)", "값": f"{_lnch_daily_clicks:.0f}회"},
                {"단계": f"→ 주문 (CVR {_lnch_cvr:.1f}%)", "값": f"{_lnch_daily_orders:.1f}건"},
                {"단계": "→ 매출", "값": fu(_lnch_daily_orders * _lnch_price)},
            ])
            st.dataframe(_lnch_funnel, use_container_width=True, hide_index=True)

            # 인사이트
            if _lnch_profit >= 0:
                st.success(
                    f"✅ 이 가정이면 **월 {fu(_lnch_profit)} 흑자**입니다. "
                    f"ACoS {_lnch_acos:.1f}%로 {'목표 이내' if _lnch_acos <= TARGET_ACOS else '목표 초과이나 흑자'}."
                )
            else:
                _months_to_be = "—"
                # CVR이 매월 20% 개선된다고 가정하면 몇 개월 후 흑자?
                _sim_cvr_temp = _lnch_cvr
                for m in range(1, 13):
                    _sim_cvr_temp *= 1.20
                    _sim_ord_temp = (_lnch_daily_budget / _lnch_cpc) * (_sim_cvr_temp / 100) * 30
                    _sim_sales_temp = _sim_ord_temp * _lnch_price
                    _sim_profit_temp = _sim_sales_temp - _sim_ord_temp * _lnch_cogs - _lnch_budget
                    if _sim_profit_temp >= 0:
                        _months_to_be = f"{m}개월"
                        break
                st.warning(
                    f"⚠️ 이 가정이면 **월 {fu(abs(_lnch_profit))} 적자**입니다.  \n"
                    f"CVR이 매월 20%씩 개선된다면 약 **{_months_to_be}** 후 흑자 전환 예상.  \n"
                    f"신제품 런칭 초기 적자는 정상입니다. 핵심은 CVR·CTR 개선 추세입니다."
                )
            st.divider()

    # ── 광고 퍼널 ──────────────────────────────────────────────────
    sec("② 광고 퍼널")
    _fc1,_fc2=st.columns([1,1])
    with _fc1:
        fig_f=go.Figure(go.Funnel(
            y=[f"노출\n{fi(_IMPR)}",f"클릭\n{fi(_CLICKS)}  CTR {fp(_CTR)}",f"주문\n{fi(_ORDERS)}  CVR {fp(_CVR_ORD)}"],
            x=[_IMPR,_CLICKS,_ORDERS],textposition="inside",textinfo="value+percent previous",
            marker=dict(color=["#3B82F6","#10B981","#8B5CF6"]),
            connector=dict(line=dict(color="#E2E8F0",width=2))))
        fig_f.update_layout(**{**BASE_LAYOUT,"height":260,"title":"광고 퍼널","margin":dict(l=4,r=4,t=40,b=4)})
        st.plotly_chart(fig_f,use_container_width=True)
    with _fc2:
        st.markdown("##### 📍 퍼널 병목 진단")
        st.markdown(f"""
| 구간 | 현재 | 기준 | 상태 |
|------|------|------|------|
| CTR(노출→클릭) | {fp(_CTR)} | ≥0.35% | {"✅" if _CTR>=0.35 else "🔴 낮음"} |
| CVR(클릭→주문) | {fp(_CVR_ORD)} | ≥10% | {"✅" if _CVR_ORD>=10 else "🔴 낮음"} |
| 노출→주문 | {fp(safe_div(_ORDERS,_IMPR,pct=True))} | — | 전체 효율 |
""")
    st.divider()

    # ── 일별/주별 트렌드 ───────────────────────────────────────────
    sec("③ 일별/주별 트렌드")
    _freq_lbl=st.radio("집계 단위",["일별","주별","월별"],horizontal=True,key="t1_freq")
    _freq={"일별":"D","주별":"W","월별":"ME"}[_freq_lbl]
    _res=(df.set_index("date").resample(_freq)
          .agg({c:"sum" for c in _SUM_COLS if c in df.columns}).reset_index())
    _res=_recalc_ratios(_res)
    _res["_lbl"]=(_res["date"].dt.strftime("%m/%d") if _freq=="D"
                  else _res["date"].dt.strftime("%Y-W%U") if _freq=="W"
                  else _res["date"].dt.strftime("%Y-%m"))
    fig_trend=make_subplots(specs=[[{"secondary_y":True}]])
    fig_trend.add_trace(go.Bar(x=_res["_lbl"],y=_res["spend"],name="Spend($)",marker_color=C["spend"],opacity=.8),secondary_y=False)
    fig_trend.add_trace(go.Bar(x=_res["_lbl"],y=_res["sales"],name="Sales($)",marker_color=C["sales"],opacity=.8),secondary_y=False)
    fig_trend.add_trace(go.Scatter(x=_res["_lbl"],y=_res["acos_pct"],name="ACoS(%)",
                                    mode="lines+markers",line=dict(color=C["acos"],width=2.5)),secondary_y=True)
    fig_trend.add_hline(y=TARGET_ACOS,line_dash="dash",line_color="#EF4444",secondary_y=True)
    fig_trend.update_layout(**{**BASE_LAYOUT,"barmode":"group","height":340,"title":"Spend / Sales / ACoS"})
    fig_trend.update_yaxes(title_text="USD ($)",secondary_y=False)
    fig_trend.update_yaxes(title_text="ACoS (%)",secondary_y=True)
    st.plotly_chart(fig_trend,use_container_width=True)
    st.divider()

    # ── 캠페인 테이블 ──────────────────────────────────────────────
    sec("④ 캠페인 성과 테이블")
    st.caption(f"🚦 ACoS: 🟢≤{TARGET_ACOS:.0f}%  🟡≤{ACOS_WARN:.0f}%  🔴>{ACOS_WARN:.0f}%")
    cg=agg_kpis(df,["campaign","campaign_type"])
    cg=sort_df(cg,SORT_METRIC,SORT_ASC)
    st.plotly_chart(build_campaign_table_fig(cg,TARGET_ACOS),use_container_width=True)
    st.download_button("📥 캠페인 CSV",cg.to_csv(index=False).encode("utf-8-sig"),"campaign_kpi.csv",key="dl_camp")
    st.divider()

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # ④-b Ad Group별 성과
    sec("④-b Ad Group별 성과")
    if "ad_group" in df.columns and df["ad_group"].notna().any():
        _ag = agg_kpis(df, ["campaign", "ad_group"])
        _ag = sort_df(_ag, SORT_METRIC, SORT_ASC)
        _ag_cols = ["campaign", "ad_group", "spend", "sales", "orders", "clicks",
                    "acos_pct", "roas", "cpc", "cvr_pct"]
        _ag_cols = [c for c in _ag_cols if c in _ag.columns]
        _ag_fmt = _ag[_ag_cols].copy()
        for _mc in ["spend", "sales"]:
            if _mc in _ag_fmt.columns: _ag_fmt[_mc] = _ag_fmt[_mc].map(fu)
        for _pc in ["acos_pct", "cvr_pct"]:
            if _pc in _ag_fmt.columns: _ag_fmt[_pc] = _ag_fmt[_pc].map(fp)
        if "roas" in _ag_fmt.columns: _ag_fmt["roas"] = _ag_fmt["roas"].map(fx)
        if "cpc" in _ag_fmt.columns: _ag_fmt["cpc"] = _ag_fmt["cpc"].map(fu)
        for _ic in ["orders", "clicks"]:
            if _ic in _ag_fmt.columns: _ag_fmt[_ic] = _ag_fmt[_ic].map(fi)
        st.dataframe(_ag_fmt.rename(columns={
            "campaign": "캠페인", "ad_group": "Ad Group", "spend": "Spend($)",
            "sales": "Sales($)", "orders": "주문", "clicks": "클릭",
            "acos_pct": "ACoS(%)", "roas": "ROAS", "cpc": "CPC($)", "cvr_pct": "CVR(%)"}),
            use_container_width=True, hide_index=True, height=340)
        st.download_button("📥 Ad Group CSV", _ag.to_csv(index=False).encode("utf-8-sig"),
                           "adgroup_kpi.csv", key="dl_adgroup")
    else:
        st.info("Ad Group 데이터가 없습니다.")
    st.divider()

    # ── Keyword 분석 ───────────────────────────────────────────────
    sec("⑤ 검색어 분석")
    _sq=agg_kpis(df,["search_term","campaign"])
    _sq=sort_df(_sq,SORT_METRIC,SORT_ASC)
    _kw_cols=["search_term","campaign","spend","sales","orders","clicks","acos_pct","roas","cpc","cvr_pct"]
    _kw_cols=[c for c in _kw_cols if c in _sq.columns]
    _sq_t1,_sq_t2=st.tabs([f"📋 Top 100",f"🚫 Negative 후보"])
    with _sq_t1:
        st.dataframe(_sq.head(100)[_kw_cols].rename(columns={
            "search_term":"검색어","campaign":"캠페인","spend":"Spend($)","sales":"Sales($)",
            "orders":"주문","clicks":"클릭","acos_pct":"ACoS(%)","roas":"ROAS","cpc":"CPC($)","cvr_pct":"CVR(%)"}),
            use_container_width=True,hide_index=True,height=340)
    with _sq_t2:
        _neg=_sq[(_sq["clicks"]>=MIN_CLICKS)&(_sq["orders"]==0)]
        if _neg.empty: st.success(f"Negative 후보 없음 (클릭≥{MIN_CLICKS} + 주문=0 없음)")
        else:
            st.error(f"🚨 {len(_neg)}개 — 낭비 추정 {fu(float(_neg['spend'].sum()))}")
            st.dataframe(_neg[["search_term","campaign","clicks","spend"]].rename(
                columns={"search_term":"검색어","campaign":"캠페인","clicks":"클릭","spend":"낭비($)"}
                ).sort_values("낭비($)",ascending=False),use_container_width=True,hide_index=True)
            st.download_button("📥 Negative CSV",
                               _neg[["search_term","campaign","clicks","spend"]].to_csv(index=False).encode("utf-8-sig"),
                               "negative_keywords.csv",key="dl_neg1")
    st.divider()

    # ── Placement 성과 ─────────────────────────────────────────────
    sec("⑥ 노출위치별 성과")
    with st.expander("📖 노출위치별 성과는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **3가지 위치:** Top of Search(CTR↑ CPC↑), Rest of Search(보통), Product Page(CTR↓ 볼륨↑)

    **핵심:** 위치별 ACoS를 비교. 효율 좋은 위치에 집중, 비효율 위치 Bid↓.
    """)
    if df["placement"].notna().any():
        _pg=agg_kpis(df,["placement"])
        _pc1,_pc2=st.columns(2)
        with _pc1:
            fig_pl=bar_line_fig(_pg["placement"],
                {"Spend($)":(_pg["spend"],C["spend"]),"Sales($)":(_pg["sales"],C["sales"])},
                title="노출위치별 Spend/Sales",height=270)
            st.plotly_chart(fig_pl,use_container_width=True)
        with _pc2:
            fig_pl2=go.Figure()
            fig_pl2.add_trace(go.Bar(x=_pg["placement"],y=_pg["acos_pct"],
                                     name="ACoS(%)",marker_color=C["acos"],
                                     text=_pg["acos_pct"].map(fp),textposition="outside"))
            fig_pl2.add_hline(y=TARGET_ACOS,line_dash="dash",line_color="#EF4444")
            fig_pl2.update_layout(**{**BASE_LAYOUT,"height":270,"title":"노출위치별 ACoS"})
            st.plotly_chart(fig_pl2,use_container_width=True)
    st.divider()

    # ── 드릴다운 ───────────────────────────────────────────────────
    sec("⑦ 드릴다운 (캠페인 → SKU → 검색어)")
    _L0=agg_kpis(df,["campaign","campaign_type"])
    _camp_choices=["— 선택 안 함 —"]+sort_df(_L0,SORT_METRIC,SORT_ASC)["campaign"].tolist()
    dd_camp=st.selectbox("캠페인 선택",_camp_choices,key="dd_camp_sel")
    if dd_camp!="— 선택 안 함 —":
        _df_c=df[df["campaign"]==dd_camp]
        _disp_key="sku" if _df_c["sku"].notna().any() else "asin"
        _L1=agg_kpis(_df_c,[_disp_key,"asin"])
        _L1=sort_df(_L1,SORT_METRIC,SORT_ASC)
        _L1c=[_disp_key,"asin","spend","sales","orders","roas","acos_pct","cpc","cvr_pct"]
        _L1c=[c for c in _L1c if c in _L1.columns]
        st.dataframe(_L1[_L1c].rename(columns={_disp_key:"SKU","asin":"ASIN","spend":"Spend($)",
            "sales":"Sales($)","orders":"주문","roas":"ROAS","acos_pct":"ACoS(%)","cpc":"CPC($)","cvr_pct":"CVR(%)"}),
            use_container_width=True,hide_index=True,height=260)
        _sku_choices=["— 선택 안 함 —"]+_L1[_disp_key].dropna().tolist()
        dd_sku2=st.selectbox(f"{_disp_key.upper()} 선택 → 검색어",_sku_choices,key="dd_sku_sel")
        if dd_sku2!="— 선택 안 함 —":
            _df_s=_df_c[_df_c[_disp_key]==dd_sku2]
            _L2=agg_kpis(_df_s,["search_term","placement"])
            _L2=sort_df(_L2,SORT_METRIC,SORT_ASC)
            st.dataframe(_L2.head(50)[["search_term","placement","spend","sales","orders","roas","acos_pct"]].rename(
                columns={"search_term":"검색어","placement":"노출위치","spend":"Spend($)",
                         "sales":"Sales($)","orders":"주문","roas":"ROAS","acos_pct":"ACoS(%)"}),
                use_container_width=True,hide_index=True,height=260)
    st.divider()

    # ── Target 유형 ────────────────────────────────────────────────
    sec("⑧ Target 유형 성과 비교")
    if "target_group" in df.columns and df["target_group"].notna().any():
        tg_agg=agg_kpis(df,["target_group","campaign_type"])
        tg_agg=sort_df(tg_agg,SORT_METRIC,SORT_ASC)
        if not tg_agg.empty:
            fig_tg=bar_line_fig(tg_agg["target_group"],
                {"Spend($)":(tg_agg["spend"],C["spend"]),"Sales($)":(tg_agg["sales"],C["sales"])},
                title="타겟유형별 Spend/Sales",height=270)
            st.plotly_chart(fig_tg,use_container_width=True)
            _tgc=["target_group","campaign_type","spend","sales","orders","acos_pct","roas","cpc","cvr_pct"]
            _tgc=[c for c in _tgc if c in tg_agg.columns]
            st.dataframe(tg_agg[_tgc].rename(columns={
                "target_group":"타겟유형","campaign_type":"유형","spend":"Spend($)","sales":"Sales($)",
                "orders":"주문","acos_pct":"ACoS(%)","roas":"ROAS","cpc":"CPC($)","cvr_pct":"CVR(%)"}),
                use_container_width=True,hide_index=True)

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # ⑨ 기간 비교 — 전반 vs 후반
    st.divider()
    sec("⑨ 기간 비교 — 전반 vs 후반")
    with st.expander("📖 기간 비교는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** 전반기 vs 후반기 성과 비교로 개선/악화 추세 확인

    **판단:** 후반기 ACoS↓ ROAS↑ → 개선 추세(좋음). 반대 → 악화(주의).
    """)
    _all_dates = sorted(df["date"].dropna().unique())
    _n_dates = len(_all_dates); _mid = _n_dates // 2
    _h1 = _all_dates[:_mid] if _mid > 0 else _all_dates
    _h2 = _all_dates[_mid:] if _mid > 0 else _all_dates
    def _pkpi(dates):
        _d = df[df["date"].isin(dates)]
        if _d.empty: return {}
        sp=float(_d["spend"].sum()); sa=float(_d["sales"].sum())
        cl=float(_d["clicks"].sum()); od=float(_d["orders"].sum())
        return dict(spend=sp, sales=sa, orders=od, clicks=cl,
                    roas=safe_div(sa,sp), acos=safe_div(sp,sa,pct=True),
                    cpc=safe_div(sp,cl), cvr=safe_div(od,cl,pct=True))
    _p1 = _pkpi(_h1); _p2 = _pkpi(_h2)
    if _p1 and _p2:
        _cc_hp = st.columns(4)
        for i, (_m, _l, _f) in enumerate([
            ("roas","ROAS",lambda v:f"{v:.2f}x"), ("acos","ACoS(%)",lambda v:f"{v:.1f}%"),
            ("cpc","CPC($)",lambda v:f"${v:.2f}"), ("cvr","CVR(%)",lambda v:f"{v:.1f}%"),
        ]):
            _v1=_p1.get(_m,0); _v2=_p2.get(_m,0); _d=_v2-_v1
            _better = (_m in ("roas","cvr") and _d>0) or (_m in ("acos","cpc") and _d<0)
            _cc_hp[i].metric(_l, _f(_v2), f"{'+' if _d>=0 else ''}{_f(_d)} vs 전반",
                             delta_color="normal" if _better else "inverse")

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # ⑩ 데이터 품질 리포트
    st.divider()
    sec("⑩ 데이터 품질 리포트")
    _dq_items = []
    _n_days_dq = df["date"].nunique()
    _dq_items.append({"항목": "총 행수", "값": f"{len(df):,}", "상태": "✅"})
    _dq_items.append({"항목": "기간 (일수)", "값": f"{D0.date()} ~ {D1.date()} ({_n_days_dq}일)", "상태": "✅"})

    _critical_cols_dq = ["spend", "sales", "clicks", "impressions", "orders"]
    for c in _critical_cols_dq:
        if c in df.columns:
            na_pct = df[c].isna().mean() * 100
            _dq_items.append({"항목": f"{c} NULL(%)", "값": f"{na_pct:.1f}%",
                              "상태": "✅" if na_pct < 1 else "⚠" if na_pct < 5 else "❌"})

    _zero_rows_dq = df[_critical_cols_dq].sum(axis=1) == 0
    _zero_pct_dq = _zero_rows_dq.mean() * 100
    _dq_items.append({"항목": "전 metric 0인 행 (%)", "값": f"{_zero_pct_dq:.1f}%",
                      "상태": "✅" if _zero_pct_dq < 10 else "⚠" if _zero_pct_dq < 30 else "❌"})

    if HAS_COGS:
        _cmr_dq = cogs_match_rate(df)
        _dq_items.append({"항목": "ASIN↔원가 매칭(%)", "값": f"{_cmr_dq['matched_skus']}/{_cmr_dq['total_skus']} ({_cmr_dq['match_pct']:.0f}%)",
                          "상태": "✅" if _cmr_dq["match_pct"] > 80 else "⚠" if _cmr_dq["match_pct"] > 50 else "❌"})

    _dq_items.append({"항목": "캠페인 수", "값": f"{df['campaign'].nunique()}개", "상태": "✅"})
    _dq_items.append({"항목": "고유 검색어 수", "값": f"{df['search_term'].nunique():,}개", "상태": "✅"})
    _dq_items.append({"항목": "원가 마스터", "값": "연결됨" if HAS_COGS else "미연결",
                      "상태": "✅" if HAS_COGS else "ℹ"})

    _dq_df = pd.DataFrame(_dq_items)
    _n_issues_dq = sum(1 for s in _dq_df["상태"] if s in ("⚠", "❌"))
    if _n_issues_dq == 0:
        st.success("✅ 데이터 품질 양호 — 이슈 없음")
    else:
        st.warning(f"⚠ {_n_issues_dq}개 항목에 주의 필요")
    st.dataframe(_dq_df, use_container_width=True, hide_index=True)


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 2 — CEO 전략 리포트                                        ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB2:
    sec("📌 CEO 핵심 요약")
    _c1,_c2,_c3,_c4,_c5=st.columns(5)
    kpi_card(_c1,"광고비(Spend,$)",fu(_SPEND),color="blue")
    kpi_card(_c2,"광고매출(Sales,$)",fu(_SALES),color="green")
    _rcls="kpi-good" if _ROAS>=_BE_ROAS_TOT else "kpi-bad"
    kpi_card(_c3,"ROAS(x)",fx(_ROAS),f"BE {fx(_BE_ROAS_TOT)} {'✅' if _ROAS>=_BE_ROAS_TOT else '❌'}",sub_cls=_rcls,color="purple")
    kpi_card(_c4,"영업이익($)",fu(_PROFIT),"🟢 흑자" if _PROFIT>0 else "🔴 적자",
             sub_cls="kpi-good" if _PROFIT>0 else "kpi-bad",color="green" if _PROFIT>0 else "red")
    kpi_card(_c5,"BE ACoS(%)",fp(_BE_ACOS_TOT),f"현재 ACoS {fp(_ACOS)}",
             sub_cls="kpi-good" if _ACOS<=_BE_ACOS_TOT else "kpi-bad",color="slate")
    st.divider()

    # ── Verdict ──────────────────────────────────────────────────
    sec("📌 Executive Verdict")
    with st.expander("📖 Executive Verdict는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **4단계 판정 기준:**
    - 🟢 **SCALE:** ROAS가 BE의 120%+ → 예산 증액해도 흑자 유지
    - 🟡 **HOLD:** ROAS가 BE의 90~120% → 현행 유지하면서 효율 개선
    - 🔧 **FIX:** ROAS가 BE의 70~90% → Bid/타겟 최적화 필요
    - 🔴 **CUT:** ROAS가 BE의 70% 미만 → 즉시 Pause 또는 대폭 축소

    **핵심:** CUT 비중이 높으면 구조적 문제. SCALE 캠페인에 예산을 집중하세요.
    """)
    if IS_GROWTH_MODE:
        # 성장 모드: ACoS 중심 판단
        if _ACOS <= TARGET_ACOS:
            _vt = f"🟢 광고 효율 양호 — ACoS {_ACOS:.1f}% (목표 {TARGET_ACOS:.0f}% 이내)"
            _vd = (f"CTR {fp(_CTR)} · CVR {fp(_CVR_ORD)} · CPC {fu(_CPC)}. "
                   "광고 효율이 좋습니다. 예산 증액으로 노출·매출 확대를 검토하세요."); _vc = "#10B981"
        elif _ACOS <= ACOS_WARN:
            _vt = f"🟡 ACoS 주의 — {_ACOS:.1f}% (목표 {TARGET_ACOS:.0f}% 초과)"
            _vd = (f"CTR {fp(_CTR)} · CVR {fp(_CVR_ORD)}. "
                   "신제품 초기라면 정상 범위. 리스팅 최적화로 CVR 개선 시 ACoS 자연 하락 기대."); _vc = "#F59E0B"
        else:
            _vt = f"🔴 ACoS 과다 — {_ACOS:.1f}% (목표의 {_ACOS/TARGET_ACOS:.1f}배)"
            _vd = (f"CTR {fp(_CTR)} · CVR {fp(_CVR_ORD)} · CPC {fu(_CPC)}. "
                   "비효율 키워드 정리 + Bid 인하 + 리스팅/리뷰 강화 필요."); _vc = "#EF4444"
    else:
        # 수익 모드: 영업이익 중심 판단
        _pct_gap=(_ROAS-_BE_ROAS_TOT)/_BE_ROAS_TOT*100 if _BE_ROAS_TOT>0 else 0
        if _PROFIT>0 and _ROAS>=_BE_ROAS_TOT:
            _vt=f"🟢 흑자 운영 중 — ROAS {_ROAS:.2f}x (BE {_BE_ROAS_TOT:.2f}x 대비 +{_pct_gap:.0f}%)"
            _vd="광고 효율 양호. SCALE 가능한 캠페인에 예산 확대 검토 권고."; _vc="#10B981"
        elif _PROFIT>0:
            _vt=f"🟡 흑자이나 여유 부족 — ROAS {_ROAS:.2f}x (BE {_BE_ROAS_TOT:.2f}x 근접)"
            _vd="CPC 상승 시 적자 전환 위험. 비효율 캠페인 정리 우선."; _vc="#F59E0B"
        else:
            _lp=abs(_PROFIT)/_SPEND*100 if _SPEND>0 else 0
            _vt=f"🔴 구조적 적자 — 영업이익 {fu(_PROFIT)} (광고비의 {_lp:.0f}% 손실)"
            _vd=f"ROAS {_ROAS:.2f}x < BE {_BE_ROAS_TOT:.2f}x. 현재 구조에서 광고 확대는 손실 확대."; _vc="#EF4444"
    st.markdown(f'<div style="background:#F8FAFC;border-left:4px solid {_vc};padding:16px 20px;border-radius:0 8px 8px 0;margin:8px 0"><h4>{_vt}</h4><p>{_vd}</p></div>',unsafe_allow_html=True)

    # 성장 모드 추가 안내
    if IS_GROWTH_MODE and _PROFIT < 0:
        st.info(
            f"💡 **성장 모드 참고**: 현재 영업이익 {fu(_PROFIT)}(적자)이지만, "
            f"신제품 런칭 초기에는 정상입니다. "
            f"ACoS가 목표({TARGET_ACOS:.0f}%) 이내로 안정되면 흑자 전환이 기대됩니다."
        )
    st.divider()

    # ── Core Issues 자동 진단 ─────────────────────────────────────
    sec("⚠️ Core Issue 자동 진단")
    with st.expander("📖 Core Issue 진단은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **진단 항목:** CPC 과다(AOV의 5%+), CVR 부족(8% 미만), ROAS < BE(구조적 적자)

    **활용:** 빨간색 항목부터 우선 해결. 여러 문제가 겹치면 CVR 개선이 가장 효과적.
    """)
    _cg_pnl=agg_kpis(df,["campaign"])
    _cg_pnl["verdict"]=_cg_pnl.apply(lambda r:_apply_verdict(r,_BE_ROAS_TOT,IS_GROWTH_MODE,TARGET_ACOS),axis=1)
    _n_losers=int((_cg_pnl["operating_profit"]<0).sum())
    _loser_loss=float(_cg_pnl[_cg_pnl["operating_profit"]<0]["operating_profit"].sum())
    issues=[]
    if _CPC>0 and _AOV>0 and _CPC/_AOV*100>5: issues.append(f"💸 CPC 과다: CPC {fu(_CPC)}가 AOV {fu(_AOV)}의 {_CPC/_AOV*100:.1f}% 차지")
    if _CVR_ORD<5: issues.append(f"📉 전환율 부진: CVR {_CVR_ORD:.1f}% — 업계 평균(~8%) 하회")
    if _ACOS>TARGET_ACOS*1.5: issues.append(f"📊 ACoS 과다: {_ACOS:.1f}% — 목표 {TARGET_ACOS:.0f}% 대비 {_ACOS-TARGET_ACOS:.1f}%p 초과")
    if _n_losers>0: issues.append(f"🚨 적자 캠페인 {_n_losers}개: 합산 손실 {fu(_loser_loss)}")
    if issues:
        for _iss in issues:
            st.markdown(f'<div style="background:#FFFBEB;border-left:4px solid #F59E0B;padding:12px 16px;margin:6px 0;border-radius:0 6px 6px 0;font-size:13px">{_iss}</div>',unsafe_allow_html=True)
    else:
        st.success("✅ 주요 구조적 문제 없음")
    st.divider()

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # "왜 ROAS X인데 적자인가?" 교육 블록
    if _PROFIT < 0:
        sec(f"💡 왜 ROAS {_ROAS:.2f}인데 적자인가요?")
        st.markdown(f"""
**영업이익 공식 (all-in 원가 기반):**
```
영업이익 = Sales − Units × unit_cogs_adj − Spend
         = {fu(_SALES)} − {fi(_UNITS)} × {fu(_AVG_COGS_PU)} − {fu(_SPEND)}
         = {fu(_SALES)} − {fu(_TOTAL_COGS_ADJ)} − {fu(_SPEND)}
         = {fu(_PROFIT)}
```
**핵심**: 단위당 원가(unit_cogs_adj)가 {fu(_AVG_COGS_PU)}이고, {fi(_UNITS)}개를 팔았으므로 총 원가가 {fu(_TOTAL_COGS_ADJ)}입니다.
여기에 광고비 {fu(_SPEND)}를 빼면 남는 게 {fu(_PROFIT)}입니다.

**BE ROAS {fx(_BE_ROAS_TOT) if _BE_ROAS_TOT < 900 else '∞'}의 의미**: ROAS가 이 이상이어야 비로소 영업이익 $0.
현재 {fx(_ROAS)}이므로 {'구조적 적자 — 광고비 0원이어도 원가가 매출 초과' if _BE_ROAS_TOT >= 900 else f'갭이 {_BE_ROAS_TOT - _ROAS:.2f}'}입니다.
""")
        st.divider()

    # ── Top Losers / Winners ──────────────────────────────────────
    sec("📉 Top Losers / Winners (캠페인)")
    with st.expander("📖 Top Losers/Winners는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **Losers:** 영업이익 적자가 가장 큰 캠페인 → Pause 또는 Bid 인하 대상
    **Winners:** 영업이익이 가장 큰 캠페인 → 예산 증액(SCALE) 1순위

    **핵심:** Losers 제거만으로 전체 이익이 크게 개선될 수 있습니다.
    """)
    _lw1,_lw2=st.columns(2)
    for _col,_title,_asc in [(_lw1,"📉 Top Losers",True),(_lw2,"📈 Top Winners",False)]:
        with _col:
            st.markdown(f"##### {_title}")
            _t=_cg_pnl.sort_values("operating_profit",ascending=_asc).head(10)
            _td=_t[["campaign","spend","sales","roas","operating_profit","verdict"]].copy()
            for _c2 in ["spend","sales","operating_profit"]: _td[_c2]=_td[_c2].map(fu)
            _td["roas"]=_td["roas"].map(fx)
            st.dataframe(_td.rename(columns={"campaign":"캠페인","spend":"Spend","sales":"Sales",
                         "roas":"ROAS","operating_profit":"영업이익","verdict":"Verdict"}),
                use_container_width=True,hide_index=True)
    st.divider()

    # ── Sub-tabs: BEP, Levers, Pause Sim, Budget Realloc, 목표 역산 ─
    _t2a,_t2b,_t2c,_t2d,_t2e=st.tabs(["📈 손익분기(BEP)","🎯 KPI 레버","🚫 Pause 시뮬","🔄 예산 재배분","🎯 목표 역산"])

    with _t2a:
        with st.expander("📖 손익분기(BEP) 분석은 어떻게 보나요?", expanded=False):
            st.markdown("""
**목적:** ROAS가 얼마일 때 영업이익이 0이 되는지 (손익분기점) 찾기

**차트 해석:**
- X축: ROAS, Y축: 영업이익
- 빨간 점선이 0을 넘는 지점이 **BE ROAS** (손익분기 ROAS)
- 현재 ROAS가 BE ROAS보다 높으면 → 흑자, 낮으면 → 적자

**BE ACoS:** BE ROAS의 역수 (= 1/BE ROAS × 100%). ACoS가 이 값 이하여야 흑자.
""")
        _base_cogs_pu = _AVG_COGS_PU   # units-weighted: sum(units×cogs_adj)/sum(units)
        _upo_bep      = _UNITS / max(_ORDERS, 1)   # units-per-order baseline ratio
        _roas_range   = np.arange(1.0, 12.1, 0.2)
        # profit(ROAS) = sales(r) - units(r)×avg_cogs - spend
        # sales(r)  = SPEND × r
        # orders(r) = sales(r) / AOV   (AOV unchanged)
        # units(r)  = orders(r) × units_per_order
        def _bep_profit(r):
            sa_r  = _SPEND * r
            ord_r = sa_r / _AOV if _AOV > 0 else 0
            un_r  = ord_r * _upo_bep
            return sa_r - un_r * _base_cogs_pu - _SPEND

        _pline = [_bep_profit(r) for r in _roas_range]
        fig_bep = go.Figure()
        fig_bep.add_trace(go.Scatter(x=_roas_range, y=_pline, mode="lines", fill="tozeroy",
                                      fillcolor="rgba(16,185,129,.10)",
                                      line=dict(color=C["sales"], width=3), name="영업이익($)"))
        fig_bep.add_hline(y=0, line_dash="dash", line_color="#EF4444", annotation_text="Profit=$0")
        fig_bep.add_vline(x=_BE_ROAS_TOT if _BE_ROAS_TOT < 900 else 10,
                          line_dash="dot", line_color=C["acos"],
                          annotation_text=f"BE {_BE_ROAS_TOT:.2f}" if _BE_ROAS_TOT < 900 else "BE=∞")
        fig_bep.add_vline(x=_ROAS, line_dash="dot", line_color=C["spend"],
                          annotation_text=f"현재 {_ROAS:.2f}")
        fig_bep.update_layout(**{**BASE_LAYOUT, "height": 360, "title": "ROAS vs 영업이익",
                                 "xaxis_title": "ROAS", "yaxis_title": "영업이익 ($)"})
        st.plotly_chart(fig_bep, use_container_width=True)

        # Sensitivity table
        _be_r_clamped = min(_BE_ROAS_TOT, 11.9) if _BE_ROAS_TOT < 900 else None
        _sens_points  = sorted(set(filter(None, [
            3.0, 3.5, 4.0, round(_ROAS, 1), 4.5, 5.0,
            round(_be_r_clamped, 1) if _be_r_clamped else None, 6.0
        ])))
        _sens = []
        for r in _sens_points:
            sp_r  = _bep_profit(r)
            _note = "← BE" if (_be_r_clamped and abs(r - _be_r_clamped) < 0.06) else \
                    ("← 현재" if abs(r - _ROAS) < 0.11 else "")
            _sens.append({"ROAS": f"{r:.2f}", "Sales": fu(_SPEND * r),
                          "영업이익": fu(sp_r),
                          "상태": "✅ 흑자" if sp_r > 0 else "🔴 적자", "비고": _note})
        st.dataframe(pd.DataFrame(_sens), use_container_width=True, hide_index=True)

    with _t2b:
        with st.expander("📖 KPI 레버 분석은 어떻게 보나요?", expanded=False):
            st.markdown("""
**목적:** CPC, CVR, AOV를 각각 하나만 바꿨을 때 ROAS와 영업이익이 어떻게 변하는지 비교

**단일 레버 BE 달성:** 각 지표를 **혼자서** 얼마나 바꿔야 BE에 도달하는지 보여줌
- 🟡 = 30% 이내 변화로 달성 가능 (실현 가능)
- 🔴 = 30%+ 변화 필요 (단일 레버로는 어려움, 복합 개선 필요)
""")
        st.markdown(f"**현재** — CPC {fu(_CPC)} · CVR {fp(_CVR_ORD)} · AOV {fu(_AOV)}")
        _base_cogs_pu2=_AVG_COGS_PU   # units-weighted avg cogs
        _lc1,_lc2,_lc3=st.columns(3)
        with _lc1: _cvr_d=st.slider("CVR 변화(%pt)",-3.0,6.0,1.0,0.5,key="lv_cvr")
        with _lc2: _cpc_d=st.slider("CPC 변화(%)",-40,20,-15,5,key="lv_cpc")
        with _lc3: _aov_d=st.slider("AOV 변화(%)",-20,40,10,5,key="lv_aov")
        def _lsim(ca,pa,aa):
            nc  = _CVR_ORD/100 + ca/100          # new CVR (rate)
            np_ = _CPC * (1 + pa/100)             # new CPC
            na  = _AOV * (1 + aa/100)             # new AOV
            ns  = np_ * _CLICKS                   # new spend
            no  = _CLICKS * nc                    # new orders
            nsa = no * na                         # new sales
            nr  = safe_div(nsa, ns)               # new ROAS
            # units = orders × (avg_units_per_order)
            # avg_units_per_order = _UNITS / _ORDERS (stable ratio assumption)
            _upo = _UNITS / max(_ORDERS, 1)       # units per order (baseline)
            n_units = no * _upo                   # new units
            # profit = sales - units×avg_cogs - spend
            nop = nsa - n_units * _base_cogs_pu2 - ns
            return nr, nop
        _levers=[]
        for lbl,ca,pa,aa in [(f"CVR {_cvr_d:+.1f}%pt",_cvr_d,0,0),
                              (f"CPC {_cpc_d:+d}%",0,_cpc_d,0),
                              (f"AOV {_aov_d:+d}%",0,0,_aov_d),("🔥 복합",_cvr_d,_cpc_d,_aov_d)]:
            nr,nop=_lsim(ca,pa,aa)
            _levers.append({"레버":lbl,"ROAS":fx(nr),"영업이익":fu(nop),
                            "BE 달성":"✅" if nr>=_BE_ROAS_TOT else "❌","변화":f"${nop-_PROFIT:+,.0f}"})
        st.dataframe(pd.DataFrame(_levers),use_container_width=True,hide_index=True)

        # ===== RESTORED FROM v3_fixed (Safe Version) =====
        # 🎯 BE 달성 최소 조건 (단일 레버, all-in 원가 기반)
        st.markdown("---")
        sec("🎯 BE 달성 최소 조건 (단일 레버)")
        _upo_be = _UNITS / max(_ORDERS, 1)
        _be_conds = []
        # CPC만으로: profit = sales - units×cogs - spend = 0
        # spend_max = sales - units×cogs = be_spend
        if _CLICKS > 0 and _BE_SPEND_TOT > 0:
            _max_cpc_be = _BE_SPEND_TOT / _CLICKS
            _cpc_chg = (_max_cpc_be / _CPC - 1) * 100 if _CPC > 0 else 0
            _be_conds.append({"레버": "CPC 인하", "현재": fu(_CPC), "BE 달성값": fu(_max_cpc_be),
                              "필요 변화": f"{_cpc_chg:+.0f}%",
                              "실현 가능성": "🟡" if abs(_cpc_chg) < 30 else "🔴"})
        # CVR만으로: need more orders → more sales → profit=0
        if _CLICKS > 0 and _AVG_COGS_PU > 0:
            # sales_new - units_new×cogs - spend = 0
            # orders_new × AOV - orders_new × upo × cogs - spend = 0
            # orders_new × (AOV - upo×cogs) = spend
            _contrib_per_order = _AOV - _upo_be * _AVG_COGS_PU
            if _contrib_per_order > 0:
                _need_orders = _SPEND / _contrib_per_order
                _need_cvr = _need_orders / _CLICKS * 100
                _cvr_chg = _need_cvr - _CVR_ORD
                _be_conds.append({"레버": "CVR 향상", "현재": fp(_CVR_ORD), "BE 달성값": fp(_need_cvr),
                                  "필요 변화": f"+{_cvr_chg:.1f}%pt",
                                  "실현 가능성": "🟡" if _cvr_chg < 3 else "🔴"})
        # AOV만으로: need higher price
        if _ORDERS > 0 and _upo_be > 0:
            # orders × AOV_new - orders × upo × cogs - spend = 0
            # AOV_new = (spend + orders × upo × cogs) / orders
            _need_aov = (_SPEND + _ORDERS * _upo_be * _AVG_COGS_PU) / _ORDERS
            _aov_chg = (_need_aov / _AOV - 1) * 100 if _AOV > 0 else 0
            _be_conds.append({"레버": "AOV 향상", "현재": fu(_AOV), "BE 달성값": fu(_need_aov),
                              "필요 변화": f"+{_aov_chg:.0f}%",
                              "실현 가능성": "🟡" if _aov_chg < 30 else "🔴"})
        if _be_conds:
            st.dataframe(pd.DataFrame(_be_conds), use_container_width=True, hide_index=True)
        else:
            st.info("BE 조건 계산 불가 (데이터 부족)")

    with _t2c:
        _total_l=int((_cg_pnl["operating_profit"]<0).sum())
        if _total_l==0:
            st.success("✅ 적자 캠페인 없음")
        else:
            _pn=st.slider("Pause 적자 캠페인 수",1,max(_total_l,1),min(3,_total_l),key="t2c_pn")
            _worst=_cg_pnl.nsmallest(_pn,"operating_profit")
            _ps=float(_worst["spend"].sum()); _psa=float(_worst["sales"].sum())
            _as=_SPEND-_ps; _asa=_SALES-_psa; _ar=safe_div(_asa,_as)
            _aop=_PROFIT-float(_worst["operating_profit"].sum())
            _cmp=pd.DataFrame({"항목":["Spend","Sales","ROAS","영업이익"],
                "현재":[fu(_SPEND),fu(_SALES),fx(_ROAS),fu(_PROFIT)],
                "Pause후":[fu(_as),fu(_asa),fx(_ar),fu(_aop)],
                "변화":[fu(-_ps),fu(-_psa),f"{_ar-_ROAS:+.2f}x",f"${_aop-_PROFIT:+,.0f}"]})
            st.dataframe(_cmp,use_container_width=True,hide_index=True)
            if _aop>_PROFIT: st.success(f"✅ {_pn}개 Pause → 영업이익 ${_aop-_PROFIT:+,.0f} 개선")
            else: st.warning("⚠ 해당 캠페인이 매출도 기여 — Pause 효과 제한적")

    with _t2d:
        st.caption("총 예산 유지 · ROAS 비례 재배분 → 영업이익 최대화")
        _rl=_cg_pnl.copy()
        _rs=_rl["roas"].sum()
        _rl["roas_w"]=_rl["roas"]/_rs if _rs>0 else 1/max(len(_rl),1)
        _rl["new_spend"]=_SPEND*_rl["roas_w"]
        _rl["new_sales"]=_rl["new_spend"]*_rl["roas"]
        _base_cpu=_AVG_COGS_PU   # units-weighted avg cogs
        # new_op = new_sales - new_units×avg_cogs - new_spend
        # new_units = new_sales / AOV (assuming avg selling price stays same)
        _rl["new_units"] = (_rl["new_sales"] / _AOV) if _AOV > 0 else 0
        _rl["new_op"] = _rl["new_sales"] - _rl["new_units"] * _base_cpu - _rl["new_spend"]
        _new_op=float(_rl["new_op"].sum())
        _m1,_m2,_m3=st.columns(3)
        _m1.metric("현재 영업이익",fu(_PROFIT))
        _m2.metric("재배분 후 영업이익",fu(_new_op),delta=f"${_new_op-_PROFIT:+,.0f}",
                   delta_color="normal" if _new_op>_PROFIT else "inverse")
        _m3.metric("총 Spend (동일)",fu(_SPEND))
        _rl_d=_rl[["campaign","spend","new_spend","roas","operating_profit","new_op"]].copy()
        _rl_d["Δ Spend"]=_rl_d["new_spend"]-_rl_d["spend"]
        for _c3 in ["spend","new_spend","operating_profit","new_op","Δ Spend"]: _rl_d[_c3]=_rl_d[_c3].map(fu)
        _rl_d["roas"]=_rl_d["roas"].map(fx)
        st.dataframe(_rl_d.rename(columns={"campaign":"캠페인","spend":"현재 Spend","new_spend":"재배분 Spend",
                     "roas":"ROAS","operating_profit":"현재 이익","new_op":"재배분 이익"}),
            use_container_width=True,hide_index=True)
    st.divider()

    # ── 목표 영업이익 역산 시뮬레이터 ────────────────────────────
    with _t2e:
        sec("🎯 목표 영업이익 역산 시뮬레이터")
        with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
            st.markdown("""
**목적:** "월 $X,000 영업이익을 내려면 ROAS/ACoS/매출이 얼마나 되어야 하는가?" 역산

**사용법:**
1. 목표 영업이익을 입력 (0 = 손익분기점)
2. 월 광고 예산을 설정
3. 자동으로 필요 ROAS, ACoS, 매출, 주문 수가 계산됨

**GAP 분석 해석:**
- ✅ = 현재 이미 목표 달성 중
- ❌ = 미달. GAP이 작으면 효율 개선으로 달성 가능, 크면 원가/가격 조정 필요
""")
        st.caption(
            "원하는 영업이익을 입력하면, 이를 달성하기 위한 필요 ROAS, 매출, ACoS, 주문 수를 역산합니다.  \n"
            "예: '월 $5,000 영업이익을 내려면 어떤 ROAS가 필요한가?'"
        )

        _tgt_c1, _tgt_c2 = st.columns(2)
        with _tgt_c1:
            _target_profit = st.number_input(
                "🎯 목표 영업이익 ($)", value=0.0, min_value=-10000.0, max_value=100000.0,
                step=500.0, key="t2e_target_profit",
                help="달성하고 싶은 월 영업이익. 0 = 손익분기(BEP)."
            )
        with _tgt_c2:
            _target_budget = st.number_input(
                "💰 월 광고 예산 ($)", value=round(float(_SPEND), 0),
                min_value=100.0, max_value=max(500000.0, float(_SPEND) * 2), step=500.0, key="t2e_budget",
                help="투입할 광고비. 현재 기간 Spend가 기본값."
            )

        # 역산 공식:
        # profit = sales - units × unit_cogs_adj - spend
        # sales = ROAS × spend
        # units = sales / AOV × UPO (units per order ratio)
        # → profit = ROAS × spend - (ROAS × spend / AOV × UPO) × cogs_adj - spend
        # → profit = spend × (ROAS - ROAS × UPO × cogs_adj / AOV - 1)
        # → ROAS_needed = (profit + spend) / (spend × (1 - UPO × cogs_adj / AOV))

        _upo_t2e = _UNITS / max(_ORDERS, 1)
        _cost_ratio_t2e = _upo_t2e * _AVG_COGS_PU / _AOV if _AOV > 0 else 0.7
        _denom_t2e = 1 - _cost_ratio_t2e

        if _denom_t2e > 0 and _target_budget > 0:
            _needed_roas = (_target_profit + _target_budget) / (_target_budget * _denom_t2e)
            _needed_sales = _needed_roas * _target_budget
            _needed_acos = (_target_budget / _needed_sales * 100) if _needed_sales > 0 else 999
            _needed_orders = _needed_sales / _AOV if _AOV > 0 else 0
            _needed_units = _needed_orders * _upo_t2e
            _needed_daily_orders = _needed_orders / 30

            st.markdown("##### 📊 역산 결과")
            _tr1, _tr2, _tr3, _tr4, _tr5 = st.columns(5)
            _tr1.metric("필요 ROAS", f"{_needed_roas:.2f}x",
                         f"현재 {_ROAS:.2f}x → {'✅ 달성 중' if _ROAS >= _needed_roas else '❌ 미달'}",
                         delta_color="normal" if _ROAS >= _needed_roas else "inverse")
            _tr2.metric("필요 ACoS", f"{_needed_acos:.1f}%",
                         f"현재 {_ACOS:.1f}%",
                         delta_color="normal" if _ACOS <= _needed_acos else "inverse")
            _tr3.metric("필요 매출", fu(_needed_sales),
                         f"현재 {fu(_SALES)}")
            _tr4.metric("필요 주문", f"{_needed_orders:.0f}건",
                         f"일 {_needed_daily_orders:.1f}건")
            _tr5.metric("광고 예산", fu(_target_budget))

            # 손익 분해
            st.markdown("##### 💰 예상 손익 분해")
            _tgt_cogs = _needed_units * _AVG_COGS_PU
            _tgt_breakdown = pd.DataFrame([
                {"항목": "① 목표 매출", "금액": fu(_needed_sales), "비중": "100%"},
                {"항목": "② 제품원가", "금액": f"-{fu(_tgt_cogs)}", "비중": f"{_tgt_cogs/_needed_sales*100:.0f}%" if _needed_sales > 0 else "—"},
                {"항목": "③ 광고비", "금액": f"-{fu(_target_budget)}", "비중": f"{_needed_acos:.0f}%"},
                {"항목": "= 영업이익", "금액": fu(_target_profit), "비중": f"{_target_profit/_needed_sales*100:.0f}%" if _needed_sales > 0 else "—"},
            ])
            st.dataframe(_tgt_breakdown, use_container_width=True, hide_index=True)

            # GAP 분석
            st.markdown("##### 📐 현재 vs 목표 GAP")
            _gap_roas = _needed_roas - _ROAS
            _gap_sales = _needed_sales - _SALES
            _gap_orders = _needed_orders - _ORDERS

            _gap_df = pd.DataFrame([
                {"지표": "ROAS", "현재": fx(_ROAS), "목표": f"{_needed_roas:.2f}x", "GAP": f"{_gap_roas:+.2f}x",
                 "달성": "✅" if _gap_roas <= 0 else "❌"},
                {"지표": "ACoS (%)", "현재": fp(_ACOS), "목표": f"{_needed_acos:.1f}%", "GAP": f"{_ACOS - _needed_acos:+.1f}%p",
                 "달성": "✅" if _ACOS <= _needed_acos else "❌"},
                {"지표": "매출 ($)", "현재": fu(_SALES), "목표": fu(_needed_sales), "GAP": fu(_gap_sales),
                 "달성": "✅" if _gap_sales <= 0 else "❌"},
                {"지표": "주문 (건)", "현재": fi(_ORDERS), "목표": f"{_needed_orders:.0f}", "GAP": f"{_gap_orders:+.0f}",
                 "달성": "✅" if _gap_orders <= 0 else "❌"},
            ])
            st.dataframe(_gap_df, use_container_width=True, hide_index=True)

            # 달성 가능성 판단
            if _ROAS >= _needed_roas:
                st.success(
                    f"✅ **현재 ROAS({_ROAS:.2f}x)로 목표 달성 가능!**  \n"
                    f"광고비를 {fu(_target_budget)}로 유지하면 영업이익 {fu(_target_profit)} 예상."
                )
            elif _gap_roas < 1.0:
                st.warning(
                    f"⚠️ ROAS를 {_gap_roas:+.2f}x 더 올려야 합니다.  \n"
                    f"CPC 절감(-10~20%) 또는 CVR 개선(+15~30%)으로 달성 가능한 범위입니다."
                )
            else:
                st.error(
                    f"🔴 ROAS GAP이 {_gap_roas:+.2f}x로 큽니다.  \n"
                    f"광고 효율 개선만으로는 어렵습니다. 원가 절감 또는 판매가 인상을 함께 검토하세요."
                )
        else:
            st.warning("⚠️ 원가/AOV 데이터가 부족하여 역산할 수 없습니다.")
        st.divider()

    # ── CEO Summary ───────────────────────────────────────────────
    sec("📋 CEO Summary (보고서용 복사)")
    _summary=(f"[{D0.date()} ~ {D1.date()}] PPC 운영 현황\n\n"
              f"• 총 광고비 {fu(_SPEND)} 투입 → 광고매출 {fu(_SALES)} 달성 (ROAS {_ROAS:.2f}x)\n"
              f"• 영업이익(추정) {fu(_PROFIT)} ({'흑자' if _PROFIT>0 else '적자'}) — BE ROAS {_BE_ROAS_TOT:.2f} {'달성' if _ROAS>=_BE_ROAS_TOT else '미달'}\n"
              f"• ACoS {_ACOS:.1f}% (목표 {TARGET_ACOS:.0f}%) | CPC ${_CPC:.2f} | CVR {_CVR_ORD:.1f}% | AOV ${_AOV:.0f}\n"
              f"• 캠페인 {df['campaign'].nunique()}개 운영 — 적자 {_n_losers}개 (손실 {fu(abs(_loser_loss))})")
    if _n_losers>0:
        _summary+=f"\n\n[권고] 적자 {_n_losers}개 캠페인 Pause/입찰 인하 시 영업이익 약 {fu(abs(_loser_loss))} 개선 가능"
    st.text_area("보고서 텍스트",_summary,height=180,key="ceo_summary")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 3 — Profit Simulation                                      ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB3:
    _base_cpu3=_AVG_COGS_PU   # units-weighted avg cogs  (bugfix: was sum(cogs_adj)/units)

    # ── ① Incrementality 시뮬 ──────────────────────────────────────
    sec("① 광고 중단 시뮬 (Incrementality)")
    with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
        st.markdown("""
**목적:** 광고를 완전히 중단하면 매출이 얼마나 남는지 추정

**Incrementality 비율:** 사이드바에서 설정. 60%이면 "광고 매출의 60%는 광고 없이도 발생" 의미.
높을수록 광고 의존도가 낮고, 낮을수록 광고가 매출에 필수적.

**해석:** 광고 중단 후 이익이 더 크면 → 광고가 비효율. 적으면 → 광고가 수익에 기여.
""")
    _incr=st.slider("Incrementality (광고 기여 비율)",0.40,1.00,0.80,0.05,key="t3_incr",
                    help="0.80=광고 매출의 80%가 광고 순기여. 나머지 20%는 광고 없어도 발생.")
    _org_retain=1.0-_incr
    _org_sales=_SALES*_org_retain
    _org_orders=_ORDERS*_org_retain
    _org_units=_UNITS*_org_retain   # units scale proportionally with sales
    _org_op=_org_sales - (_org_units * _base_cpu3) - 0.0   # spend=0 after ad pause
    _scenarios3=[
        {"시나리오":"현재(광고 유지)","Spend($)":fu(_SPEND),"Sales($)":fu(_SALES),
         "영업이익($)":fu(_PROFIT),"ROAS":fx(_ROAS)},
        {"시나리오":"광고 전면 중단","Spend($)":fu(0),"Sales($)":fu(_org_sales),
         "영업이익($)":fu(_org_op),"ROAS":fx(0)},
        {"시나리오":f"Incrementality={_incr:.0%} 순기여","Spend($)":fu(_SPEND),
         "Sales($)":fu(_SALES*_incr),"영업이익($)":fu(_PROFIT-_org_op),"ROAS":fx(safe_div(_SALES*_incr,_SPEND))},
    ]
    st.dataframe(pd.DataFrame(_scenarios3),use_container_width=True,hide_index=True)
    if _org_op>_PROFIT:
        st.error(f"⚠️ Incrementality {_incr:.0%} 기준 — 광고 중단 시 영업이익이 오히려 ${_org_op-_PROFIT:+,.0f} 개선 가능. 광고 효율성 재검토 필요.")
    else:
        st.success(f"✅ 광고 유지가 영업이익 ${_PROFIT-_org_op:,.0f} 더 유리")
    st.divider()

    # ── ② Spend 증감 시뮬 ─────────────────────────────────────────
    sec("② Spend 증감 시나리오 (ROAS 불변 가정)")
    with st.expander("📖 이 시뮬레이션은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** 광고비를 X% 늘리거나 줄이면 매출·이익이 어떻게 변하는지 시뮬레이션

    **가정:** ROAS가 현재와 동일하게 유지 (실제로는 Spend↑ 시 ROAS↓ 경향)

    **한계:** 선형 가정이므로 참고용. 실제로는 "수확체감"이 발생합니다.
    """)
    _pct_opts3=[-30,-25,-20,-15,-10,-5,0,5,10,15,20,25,30]
    _rows3=[]
    for _p in _pct_opts3:
        _r=1+_p/100
        _sa_r=_SALES*_r; _sp_r=_SPEND*_r; _un_r=_UNITS*_r
        _op_r=_sa_r-_un_r*_base_cpu3-_sp_r
        _rows3.append({"Spend 변화":f"{_p:+d}%","Spend($)":fu(_sp_r),"Sales($)":fu(_sa_r),
                       "ROAS(x)":fx(_ROAS),"영업이익($)":fu(_op_r),
                       "상태":"✅ 흑자" if _op_r>0 else "🔴 적자","비고":"← 현재" if _p==0 else ""})
    _st3=st.slider("강조할 Spend 변화(%)",_pct_opts3[0],_pct_opts3[-1],0,5,key="t3_spend")
    st.dataframe(pd.DataFrame(_rows3),use_container_width=True,hide_index=True,height=400)
    st.caption("⚠ ROAS 불변 가정 (선형 모형). 실제로는 한계효율 체감 가능.")
    st.divider()

    # ── ③ CVR/CPC 민감도 분석 ─────────────────────────────────────
    sec("③ CVR × CPC 민감도 분석 (Heatmap)")
    with st.expander("📖 이 히트맵은 어떻게 보나요?", expanded=False):
        st.markdown("""
**목적:** CVR과 CPC가 동시에 변할 때 영업이익이 어떻게 바뀌는지 2차원 시뮬레이션

**색상 해석:** 녹색 = 흑자, 빨간색 = 적자. 현재 위치에서 녹색 방향으로 이동하는 경로를 찾으세요.

**핵심:** CPC↓ + CVR↑ 방향(좌상단)이 가장 좋은 경로. "Bid 인하 + 리스팅 개선"의 복합 효과.
""")
    _cvr_vals=np.arange(max(1,_CVR_ORD-4),_CVR_ORD+5,1)
    _cpc_vals=np.arange(max(0.1,_CPC-0.5),_CPC+0.6,0.1)
    _heat_z=[]
    _upo_t3 = _UNITS / max(_ORDERS, 1)   # units-per-order ratio
    for _cv in _cvr_vals:
        _row_h = []
        for _cp in _cpc_vals:
            _no  = _CLICKS * _cv / 100            # new orders
            _ns  = _cp * _CLICKS                  # new spend (CPC × clicks)
            _nsa = _no * _AOV                     # new sales (orders × AOV)
            _nun = _no * _upo_t3                  # new units = orders × upo
            _nop = _nsa - _nun * _base_cpu3 - _ns # profit = sales - cogs - spend
            _row_h.append(round(_nop, 0))
        _heat_z.append(_row_h)
    fig_heat=go.Figure(go.Heatmap(
        z=_heat_z,
        x=[f"${v:.2f}" for v in _cpc_vals],
        y=[f"{v:.0f}%" for v in _cvr_vals],
        colorscale="RdYlGn",
        text=[[fu(v) for v in row] for row in _heat_z],
        texttemplate="%{text}",textfont=dict(size=9),
        colorbar=dict(title="영업이익($)")))
    fig_heat.update_layout(**{**BASE_LAYOUT,"height":400,"title":"CVR × CPC 민감도 (영업이익 $)",
                              "xaxis_title":"CPC ($)","yaxis_title":"CVR (%)"})
    st.plotly_chart(fig_heat,use_container_width=True)
    st.divider()

    # ── ④ Budget 의사결정 가이드 ──────────────────────────────────
    sec("④ Budget 의사결정 가이드 (6개월 로드맵)")
    _roadmap=[
        {"월":"M+1","액션":"비효율 키워드 Negative 등록","기대 효과":"낭비 Spend 감소 → ACoS ↓"},
        {"월":"M+2","액션":"Top Losers Pause / Bid 인하","기대 효과":"구조적 적자 해소"},
        {"월":"M+3","액션":"Top Winners Spend 확대","기대 효과":"ROAS 유지하며 Sales ↑"},
        {"월":"M+4","액션":"Auto → Manual 전환 (성과 키워드)","기대 효과":"CPC 절감 + CVR ↑"},
        {"월":"M+5","액션":"LTS(장기매출) 성과 제품 집중","기대 효과":"장기 ROAS 개선"},
        {"월":"M+6","액션":"전략 재검토 (원가 마스터 업데이트)","기대 효과":"BE ACoS 재계산 정확도 ↑"},
    ]
    st.dataframe(pd.DataFrame(_roadmap),use_container_width=True,hide_index=True)
    st.divider()

    # ── [v3.2 복원] ⑤ 광고비 감축 시뮬레이션 ─────────────────────
    # 공식: op = sales×r - units×r × _base_cpu3 - spend×r  (ROAS 불변 가정)
    sec("⑤ 광고비 감축 시뮬레이션")
    with st.expander("📖 이 시뮬레이션은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** 광고비를 10~50% 줄였을 때 영업이익 변화

    **해석:** 감축 후 이익이 증가 → 현재 과다 지출. 감소 → 광고가 이익에 기여 중.
    """)
    _red_pcts = st.slider("Spend 감축 범위 (%)", 0, 80, (10, 50), 5, key="t3_red")
    _red_range = range(_red_pcts[0], _red_pcts[1]+1, 5)
    _red_rows = []
    for rp in _red_range:
        _r5 = 1 - rp/100
        ns5 = _SPEND * _r5
        nsa5 = ns5 * _ROAS
        nun5 = _UNITS * _r5
        nop5 = nsa5 - nun5 * _base_cpu3 - ns5
        _red_rows.append({"감축(%)": f"{rp}%", "Spend($)": fu(ns5), "Sales($)": fu(nsa5),
                          "Units": fi(nun5), "ROAS(x)": fx(_ROAS), "영업이익($)": fu(nop5),
                          "상태": "✅" if nop5 > 0 else "🔴"})
    st.dataframe(pd.DataFrame(_red_rows), use_container_width=True, hide_index=True)
    st.caption("⚠ ROAS 불변 가정. 실제로는 예산 삭감 시 고효율 캠페인 비중이 올라 ROAS 개선 가능.")
    st.divider()

    # ── [v3.2 복원] ⑥ CPC 경쟁 시나리오 ──────────────────────────
    sec("⑥ CPC 경쟁 시나리오")
    with st.expander("📖 이 시뮬레이션은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** 경쟁 심화로 CPC가 올라가면 영업이익이 어떻게 변하는지

    **해석:** CPC +20%에서도 흑자 → 경쟁에 강한 구조. 적자 전환 → CVR/AOV 개선 시급.
    """)
    _cpc_changes = [-20, -10, -5, 0, 5, 10, 20, 30, 50]
    _cpc_rows = []
    for pct6 in _cpc_changes:
        nc6 = _CPC * (1 + pct6/100)
        ns6 = nc6 * _CLICKS
        nsa6 = _ORDERS * _AOV
        nr6 = safe_div(nsa6, ns6)
        nop6 = nsa6 - _UNITS * _base_cpu3 - ns6
        _cpc_rows.append({"CPC변화": f"{pct6:+d}%", "CPC($)": fu(nc6), "Spend($)": fu(ns6),
                          "ROAS(x)": fx(nr6), "영업이익($)": fu(nop6),
                          "비고": "← 현재" if pct6 == 0 else ("✅" if nop6 > 0 else "🔴")})
    st.dataframe(pd.DataFrame(_cpc_rows), use_container_width=True, hide_index=True)
    st.caption("가정: 클릭 수·CVR 불변, CPC만 변동. 경쟁 입찰 변화 시뮬.")
    st.divider()

    # ── [v3.2 복원] ⑦ 경로 분해 — 레버별 독립 기여도 ─────────────
    sec("⑦ 경로 분해 — 레버별 독립 기여도")
    with st.expander("📖 경로 분해는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** CPC, CVR, AOV를 각각 10% 개선했을 때 영업이익 개선 효과를 비교

    **해석:** 막대가 긴 레버가 가장 영향력이 큼. "어디에 집중할 것인가"의 답.
    """)
    st.caption("각 지표를 1%pt/1% 변경 시 ROAS·영업이익 변화")
    _upo7 = _UNITS / max(_ORDERS, 1)
    _paths = []

    _nc7 = _CVR_ORD/100 + 0.01
    _no7 = _CLICKS * _nc7; _nu7 = _no7 * _upo7
    _nsa7 = _no7 * _AOV;  _nr7 = safe_div(_nsa7, _SPEND)
    _nop7 = _nsa7 - _nu7 * _base_cpu3 - _SPEND
    _paths.append({"레버": "CVR +1%pt", "ROAS 변화": f"{_nr7-_ROAS:+.3f}",
                    "이익 변화($)": f"${_nop7-_PROFIT:+,.0f}",
                    "민감도": "🔴 높음" if abs(_nop7-_PROFIT) > 500 else "🟡 보통"})

    _cc7 = _CPC * 0.99; _cs7 = _cc7 * _CLICKS
    _csa7 = _ORDERS * _AOV; _cr7 = safe_div(_csa7, _cs7)
    _cop7 = _csa7 - _UNITS * _base_cpu3 - _cs7
    _paths.append({"레버": "CPC -1%", "ROAS 변화": f"{_cr7-_ROAS:+.3f}",
                    "이익 변화($)": f"${_cop7-_PROFIT:+,.0f}",
                    "민감도": "🔴 높음" if abs(_cop7-_PROFIT) > 500 else "🟡 보통"})

    _aa7 = _AOV * 1.01; _asa7 = _ORDERS * _aa7; _ar7 = safe_div(_asa7, _SPEND)
    _aop7 = _asa7 - _UNITS * _base_cpu3 - _SPEND
    _paths.append({"레버": "AOV +1%", "ROAS 변화": f"{_ar7-_ROAS:+.3f}",
                    "이익 변화($)": f"${_aop7-_PROFIT:+,.0f}",
                    "민감도": "🔴 높음" if abs(_aop7-_PROFIT) > 500 else "🟡 보통"})

    _ss7 = _SPEND * 0.99; _sc7 = _CLICKS * 0.99
    _so7 = _sc7 * _CVR_ORD / 100; _su7 = _so7 * _upo7
    _ssa7 = _so7 * _AOV; _sr7 = safe_div(_ssa7, _ss7)
    _sop7 = _ssa7 - _su7 * _base_cpu3 - _ss7
    _paths.append({"레버": "Spend -1% (클릭 비례)", "ROAS 변화": f"{_sr7-_ROAS:+.3f}",
                    "이익 변화($)": f"${_sop7-_PROFIT:+,.0f}",
                    "민감도": "🔴 높음" if abs(_sop7-_PROFIT) > 500 else "🟡 보통"})

    st.dataframe(pd.DataFrame(_paths), use_container_width=True, hide_index=True)
    st.divider()

    # ── [v3.2 복원] ⑧ 6개월 영업이익 로드맵 (차트) ───────────────
    sec("⑧ 6개월 영업이익 로드맵")
    with st.expander("📖 6개월 로드맵은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **가정:** CPC 매월 3% 감소, CVR 매월 5% 증가 (현실적 개선 속도)

    **해석:** 현재 적자여도 3~4개월 후 흑자가 보이면 → 전략 유지 가치 있음.

    **활용:** CEO 보고 시 흑자 전환 근거로 사용.
    """)
    st.caption("월별 Spend 계획과 ROAS 개선 목표에 따른 누적 손익 예측")
    _rc81, _rc82 = st.columns(2)
    with _rc81:
        _avg_monthly8 = int(_SPEND / max(df["date"].dt.to_period("M").nunique(), 1))
        _m8_min = 1000; _m8_max = max(500000, _avg_monthly8 * 3)
        _m8_def = max(_m8_min, min(_avg_monthly8, _m8_max))
        _monthly_spend8 = st.number_input("월 광고비 ($)", _m8_min, _m8_max,
                                           _m8_def, step=1000, key="rm_spend")
    with _rc82:
        _roas_improve8 = st.slider("월별 ROAS 개선률 (%)", 0, 15, 3, 1, key="rm_improve")

    _upo8 = _UNITS / max(_ORDERS, 1)
    _months_rm = []
    _cum_profit8 = 0.0
    _cur_roas8 = _ROAS
    for m8 in range(1, 7):
        _cur_roas8 *= (1 + _roas_improve8/100)
        ms8 = _monthly_spend8
        msa8 = ms8 * _cur_roas8
        mo8 = msa8 / _AOV if _AOV > 0 else 0
        mu8 = mo8 * _upo8
        mop8 = msa8 - mu8 * _base_cpu3 - ms8
        _cum_profit8 += mop8
        _months_rm.append({"월": f"M{m8}", "Spend": ms8, "ROAS": _cur_roas8,
                           "Sales": msa8, "영업이익": mop8, "누적이익": _cum_profit8})

    _df_rm = pd.DataFrame(_months_rm)
    _fig_rm = make_subplots(specs=[[{"secondary_y": True}]])
    _fig_rm.add_trace(go.Bar(
        x=_df_rm["월"], y=_df_rm["영업이익"], name="월 영업이익",
        marker_color=[C["profit"] if v >= 0 else C["loss"] for v in _df_rm["영업이익"]],
        text=_df_rm["영업이익"].map(fu), textposition="outside", textfont=dict(size=9)),
        secondary_y=False)
    _fig_rm.add_trace(go.Scatter(
        x=_df_rm["월"], y=_df_rm["누적이익"], name="누적 이익",
        mode="lines+markers", line=dict(color=C["roas"], width=3)),
        secondary_y=True)
    _fig_rm.add_trace(go.Scatter(
        x=_df_rm["월"], y=_df_rm["ROAS"], name="ROAS",
        mode="lines+markers", line=dict(color=C["acos"], width=2, dash="dot"),
        visible="legendonly"),
        secondary_y=True)
    _fig_rm.add_hline(y=0, line_dash="dash", line_color="#EF4444", secondary_y=False)
    _fig_rm.update_layout(**{**BASE_LAYOUT, "height": 380, "title": "6개월 영업이익 로드맵"})
    _fig_rm.update_yaxes(title_text="월 영업이익 ($)", secondary_y=False)
    _fig_rm.update_yaxes(title_text="누적 이익 ($) / ROAS", secondary_y=True)
    st.plotly_chart(_fig_rm, use_container_width=True)

    _be_month_rm = None
    for i, row in _df_rm.iterrows():
        if row["누적이익"] >= 0 and i > 0:
            _be_month_rm = row["월"]; break
    if _cum_profit8 < 0 and _be_month_rm is None:
        st.warning(f"⚠ 6개월 내 누적 흑자 전환 불가 — 최종 누적 {fu(_cum_profit8)}")
    elif _be_month_rm:
        st.success(f"✅ **{_be_month_rm}**에 누적 흑자 전환! 최종 누적 {fu(_cum_profit8)}")
    else:
        st.success(f"✅ 이미 흑자 — 6개월 누적 {fu(_cum_profit8)}")

    _df_rm_d = _df_rm.copy()
    for c in ["Spend", "Sales", "영업이익", "누적이익"]: _df_rm_d[c] = _df_rm_d[c].map(fu)
    _df_rm_d["ROAS"] = _df_rm_d["ROAS"].map(fx)
    st.dataframe(_df_rm_d, use_container_width=True, hide_index=True)
    st.divider()

    # ── ⑩ Bid 시뮬레이터 ──
    sec("⑨ Bid 시뮬레이터 — 입찰가 변경 시 예상 성과")
    with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
        st.markdown("""
**목적:** Bid(입찰가)를 올리거나 내릴 때 노출·클릭·주문·영업이익이 어떻게 변하는지 예측

**슬라이더 설명:**
- **Bid 변경 (%):** 현재 평균 CPC 기준. +20%이면 CPC가 20% 올라갑니다
- **노출 탄력성:** Bid 1% 올릴 때 노출이 몇 % 증가하는가. 경쟁 심한 키워드(0.3~0.5)는 Bid를 올려도 노출이 적게 늘고, 니치 키워드(0.8~1.2)는 많이 늘어남
- **CVR 변화:** 공격적 입찰 시 관련성 낮은 클릭이 유입되면 CVR이 소폭 하락할 수 있음

**핵심 판단 기준:**
- 영업이익이 증가하면 → 해당 Bid 전략 적용 가치 있음
- 영업이익은 감소하지만 주문은 증가 → 성장 모드에서는 수용 가능
- 영업이익·주문 모두 감소 → 해당 Bid 전략은 부적절
""")
    st.caption(
        "Amazon PPC에서 Bid(입찰가)를 올리면 노출↑·CPC↑, 내리면 노출↓·CPC↓.  \n"
        "경쟁사 데이터가 없으므로, 현재 데이터의 Bid-노출 관계를 기반으로 근사 시뮬레이션합니다."
    )

    _bid_c1, _bid_c2, _bid_c3 = st.columns(3)
    with _bid_c1:
        _bid_change = st.slider(
            "Bid 변경 (%)", -50, 100, 0, 5, key="sim_bid_chg",
            help="현재 CPC 기준. +20% = 더 공격적 입찰, -20% = 보수적 입찰"
        )
    with _bid_c2:
        _bid_impr_elas = st.slider(
            "노출 탄력성", 0.3, 1.5, 0.7, 0.1, key="sim_bid_elas",
            help="Bid 1% 변경 시 노출이 몇 % 변하는가. 0.7 = Bid +10%이면 노출 +7%. "
                 "경쟁 심한 키워드: 0.3~0.5, 니치 키워드: 0.8~1.2"
        )
    with _bid_c3:
        _bid_cvr_drop = st.slider(
            "CVR 변화 (%)", -20, 10, 0, 1, key="sim_bid_cvr",
            help="Bid↑ 시 덜 관련성 높은 클릭 유입으로 CVR이 소폭 하락할 수 있음. "
                 "보통 -5~0%. 타겟팅이 정확하면 0."
        )

    # 시뮬레이션 계산
    _bid_new_cpc = _CPC * (1 + _bid_change / 100)
    _bid_new_impr = _IMPR * (1 + _bid_change / 100 * _bid_impr_elas)
    _bid_new_clicks = _bid_new_impr * (_CTR / 100)  # CTR 유지 가정
    _bid_new_cvr = _CVR_ORD * (1 + _bid_cvr_drop / 100)
    _bid_new_orders = _bid_new_clicks * (_bid_new_cvr / 100)
    _bid_new_spend = _bid_new_clicks * _bid_new_cpc
    _bid_new_sales = _bid_new_orders * _AOV if _AOV > 0 else 0
    _bid_new_acos = _bid_new_spend / _bid_new_sales * 100 if _bid_new_sales > 0 else 999
    _bid_new_roas = _bid_new_sales / _bid_new_spend if _bid_new_spend > 0 else 0
    _bid_new_cpa = _bid_new_spend / _bid_new_orders if _bid_new_orders > 0 else 999
    _bid_new_units = _bid_new_orders * (_UNITS / max(_ORDERS, 1))
    _bid_new_cogs = _bid_new_units * _AVG_COGS_PU
    _bid_new_profit = _bid_new_sales - _bid_new_cogs - _bid_new_spend

    st.markdown("##### 📊 Bid 시뮬레이션 결과")
    _br1, _br2, _br3, _br4, _br5 = st.columns(5)
    _br1.metric("CPC", fu(_bid_new_cpc), f"{_bid_change:+d}%", delta_color="inverse")
    _br2.metric("노출", fi(int(_bid_new_impr)), f"{(_bid_new_impr/_IMPR-1)*100:+.0f}%" if _IMPR > 0 else "")
    _br3.metric("ACoS", f"{_bid_new_acos:.1f}%", f"{_bid_new_acos-_ACOS:+.1f}%p", delta_color="inverse")
    _br4.metric("주문", fi(int(_bid_new_orders)), f"{_bid_new_orders-_ORDERS:+.0f}건")
    _br5.metric("영업이익", fu(_bid_new_profit), f"{_bid_new_profit-_PROFIT:+,.0f}", delta_color="normal" if _bid_new_profit > _PROFIT else "inverse")

    _bid_compare = pd.DataFrame([
        {"지표": "CPC ($)", "현재": fu(_CPC), "시뮬": fu(_bid_new_cpc)},
        {"지표": "노출", "현재": fi(_IMPR), "시뮬": fi(int(_bid_new_impr))},
        {"지표": "클릭", "현재": fi(_CLICKS), "시뮬": fi(int(_bid_new_clicks))},
        {"지표": "CVR (%)", "현재": fp(_CVR_ORD), "시뮬": f"{_bid_new_cvr:.1f}%"},
        {"지표": "주문", "현재": fi(_ORDERS), "시뮬": fi(int(_bid_new_orders))},
        {"지표": "Spend ($)", "현재": fu(_SPEND), "시뮬": fu(_bid_new_spend)},
        {"지표": "Sales ($)", "현재": fu(_SALES), "시뮬": fu(_bid_new_sales)},
        {"지표": "ACoS (%)", "현재": fp(_ACOS), "시뮬": f"{_bid_new_acos:.1f}%"},
        {"지표": "ROAS (x)", "현재": fx(_ROAS), "시뮬": f"{_bid_new_roas:.2f}x"},
        {"지표": "영업이익 ($)", "현재": fu(_PROFIT), "시뮬": fu(_bid_new_profit)},
    ])
    st.dataframe(_bid_compare, use_container_width=True, hide_index=True)

    if _bid_change == 0:
        st.info("👆 Bid 변경 슬라이더를 조정하여 입찰 전략을 시뮬레이션하세요.")
    elif _bid_new_profit > _PROFIT:
        st.success(f"✅ Bid {_bid_change:+d}%로 영업이익 {fu(_bid_new_profit - _PROFIT)} 증가 예상")
    else:
        st.warning(f"⚠️ Bid {_bid_change:+d}%로 영업이익 {fu(_bid_new_profit - _PROFIT)} 감소. "
                   f"노출 탄력성이 {_bid_impr_elas:.1f}보다 높은 키워드에서만 효과적입니다.")


    # ── [v3.2 복원] ⑨ 예산 결정 가이드 ───────────────────────────
    sec("⑩ 예산 결정 가이드")
    with st.expander("📖 예산 결정 가이드는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **점수 체계:** ROAS, 흑자 여부, CVR, CPC 등을 종합 채점
    - 3점+ → 📈 예산 확대 권장
    - 1~2점 → ➡️ 현행 유지
    - 0점 이하 → 📉 예산 축소 권장

    **핵심:** Profit Simulation 탭의 최종 결론입니다.
    """)
    _bscore = 0;  _breasons = []
    if _BE_ROAS_TOT < 900 and _ROAS >= _BE_ROAS_TOT * 1.2:
        _bscore += 2; _breasons.append("✅ ROAS가 BE 대비 20%+ 여유")
    elif _BE_ROAS_TOT < 900 and _ROAS >= _BE_ROAS_TOT:
        _bscore += 1; _breasons.append("🟡 ROAS가 BE 이상이나 여유 부족")
    else:
        _bscore -= 2; _breasons.append("❌ ROAS가 BE 미달 또는 구조적 적자")
    if _PROFIT > 0: _bscore += 1; _breasons.append("✅ 현재 흑자")
    else:           _bscore -= 1; _breasons.append("❌ 현재 적자")
    if _CVR_ORD > 8: _bscore += 1; _breasons.append("✅ CVR 양호 (8%+)")
    if _AOV > 0 and _CPC > _AOV * 0.05:
        _bscore -= 1; _breasons.append("⚠ CPC가 AOV의 5%+ 차지")

    if _bscore >= 3:
        _brec = "📈 **예산 확대 권장** — 효율이 좋으므로 SCALE 캠페인 위주 증액"
        _bclr = "#10B981"
    elif _bscore >= 1:
        _brec = "➡️ **현행 유지** — 효율 개선 후 증액 검토"
        _bclr = "#F59E0B"
    else:
        _brec = "📉 **예산 축소 권장** — 적자 캠페인 정리 후 재투자"
        _bclr = "#EF4444"

    st.markdown(f'<div style="border-left:4px solid {_bclr};padding:12px 16px;'
                f'background:#F9FAFB;border-radius:4px;margin:8px 0;">'
                f'<b>{_brec}</b></div>', unsafe_allow_html=True)
    for r in _breasons:
        st.markdown(f"  {r}")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 4 — 제품 구조 분석                                         ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB4:
    _BE4=_BE_ROAS_TOT

    # ── ① SKU 수익성 버블차트 ─────────────────────────────────────
    _sg_all=agg_kpis(df,["sku","asin"])
    _sg_all["be_roas_sku"]=_BE4
    _sg_all["verdict"]=_sg_all.apply(lambda r:_apply_verdict(r,_BE4,IS_GROWTH_MODE,TARGET_ACOS),axis=1)
    _sg_all["tier"]=_sg_all.apply(
        lambda r:tier_label(r["roas"],r.get("operating_profit",0),r["orders"],
                            r["spend"],_sg_all["spend"].mean() if len(_sg_all)>0 else 1,_BE4),axis=1)

    if not _sg_all.empty and _sg_all["sku"].notna().any():
        sec("① SKU별 수익성 버블차트")
        with st.expander("📖 버블차트는 어떻게 보나요?", expanded=False):
            st.markdown("""
        **축:** X=Spend, Y=ROAS, 버블 크기=Sales
        - 🟢 우상단 (High Spend+ROAS): 스타 → 유지/확대
        - 🟡 좌상단 (Low Spend+High ROAS): 숨은 보석 → 확대
        - 🟠 우하단 (High Spend+Low ROAS): 문제 → Bid↓ or Pause
        - 🔴 좌하단: 관심 불필요 → 정리
        """)
        _sg_b=_sg_all[_sg_all["sku"].notna()].copy()
        _sg_b["_lbl"]=_sg_b["sku"].fillna(_sg_b["asin"])
        _color_map={"🟢 SCALE":"#10B981","🟡 HOLD":"#F59E0B","🔧 FIX":"#3B82F6","🔴 CUT":"#EF4444"}
        fig_bubble=go.Figure()
        for vd,color in _color_map.items():
            _sub=_sg_b[_sg_b["verdict"]==vd]
            if _sub.empty: continue
            fig_bubble.add_trace(go.Scatter(
                x=_sub["roas"],y=_sub["acos_pct"],
                mode="markers+text",name=vd,
                marker=dict(size=np.clip(_sub["spend"]/max(_sub["spend"].max(),1)*60+8,8,60),
                            color=color,opacity=0.75,line=dict(color="white",width=1)),
                text=_sub["_lbl"],textposition="top center",textfont=dict(size=9),
                customdata=np.stack([_sub["spend"],_sub["sales"],_sub["operating_profit"]],axis=-1),
                hovertemplate="SKU: %{text}<br>ROAS: %{x:.2f}x<br>ACoS: %{y:.1f}%<br>"
                              "Spend: $%{customdata[0]:,.0f}<br>Sales: $%{customdata[1]:,.0f}<br>"
                              "영업이익: $%{customdata[2]:,.0f}<extra></extra>"))
        fig_bubble.add_hline(y=TARGET_ACOS,line_dash="dash",line_color="#EF4444",annotation_text=f"목표 ACoS {TARGET_ACOS:.0f}%")
        fig_bubble.add_vline(x=_BE4,line_dash="dash",line_color="#8B5CF6",annotation_text=f"BE ROAS {_BE4:.2f}")
        fig_bubble.update_layout(**{**BASE_LAYOUT,"height":500,"title":"SKU 수익성 버블차트 (크기=Spend)",
                                    "xaxis_title":"ROAS (x)","yaxis_title":"ACoS (%)"})
        st.plotly_chart(fig_bubble,use_container_width=True)
        st.divider()

    # ── ② Tier 분류표 ─────────────────────────────────────────────
    sec("② SKU Tier 분류 (T1~T4)")
    with st.expander("📖 Tier 분류는 어떻게 보나요?", expanded=False):
        st.markdown("""
    - **T1 (스타):** ROAS ≥ BE×1.2 + 흑자 → 예산 확대
    - **T2 (준수):** ROAS ≥ BE + 흑자 → 유지
    - **T3 (주의):** ROAS < BE but 주문 있음 → 개선 필요
    - **T4 (정리):** 적자 + 낮은 주문 → Pause 검토

    **활용:** T1에 예산 집중, T4 정리 → 전체 ROAS 자연 상승.
    """)
    _tier_guide={"T1 ⭐ 확장":"ROAS≥BE×1.2 + 흑자 + 주문≥2 → 예산 확대",
                 "T2 🟡 유지":"중간 성과 → 현 수준 유지/최적화",
                 "T3 🔴 적자":"ROAS<BE×0.8 + 적자 → Pause 또는 구조 개선",
                 "T4 🚫 차단":"클릭·비용 발생하나 주문=0 → 즉시 Pause"}
    for t,d in _tier_guide.items():
        _sub=_sg_all[_sg_all["tier"]==t]
        if _sub.empty: continue
        with st.expander(f"{t}  ({len(_sub)}개 SKU · Spend {fu(float(_sub['spend'].sum()))})",expanded=(t.startswith("T1") or t.startswith("T3"))):
            st.caption(d)
            _tc=["sku","asin","spend","sales","orders","roas","acos_pct","operating_profit","verdict"]
            _tc=[c for c in _tc if c in _sub.columns]
            _sd=_sub[_tc].sort_values("operating_profit").copy()
            for _mc in ["spend","sales","operating_profit"]: _sd[_mc]=_sd[_mc].map(fu) if _mc in _sd.columns else ""
            _sd["roas"]=_sd["roas"].map(fx); _sd["acos_pct"]=_sd["acos_pct"].map(fp)
            st.dataframe(_sd.rename(columns={"sku":"SKU","asin":"ASIN","spend":"Spend($)","sales":"Sales($)",
                "orders":"주문","roas":"ROAS","acos_pct":"ACoS(%)","operating_profit":"영업이익($)","verdict":"Verdict"}),
                use_container_width=True,hide_index=True)
    st.divider()

    # ── ③ 키워드 카테고리 분석 ────────────────────────────────────
    sec("③ 키워드 카테고리 (Brand / Competitor / Generic)")
    with st.expander("📖 키워드 카테고리는 어떻게 보나요?", expanded=False):
        st.markdown("""
    - **Brand:** 자사 브랜드명 → CVR 높고 ACoS 낮음
    - **Competitor:** 경쟁사명 → CVR 낮고 CPC 높음
    - **Generic:** 일반 검색어 → 볼륨 크지만 경쟁 심함

    **핵심:** Brand 비중↑ = 브랜드 파워 강함. Generic 효율이 나와야 스케일 가능.
    """)
    _kw4=agg_kpis(df,["search_term"])
    _kw4=_kw4[_kw4["clicks"]>=MIN_CLICKS].copy()
    _kw4["category"]=_kw4["search_term"].apply(_classify_kw)
    _cat4=agg_kpis(_kw4,["category"])
    if not _cat4.empty:
        _cat4c=["category","spend","sales","orders","clicks","roas","acos_pct","cpc","cvr_pct"]
        _cat4c=[c for c in _cat4c if c in _cat4.columns]
        st.dataframe(_cat4[_cat4c].rename(columns={"category":"카테고리","spend":"Spend($)","sales":"Sales($)",
            "orders":"주문","clicks":"클릭","roas":"ROAS","acos_pct":"ACoS(%)","cpc":"CPC($)","cvr_pct":"CVR(%)"}),
            use_container_width=True,hide_index=True)
        fig_cat=bar_line_fig(_cat4["category"],
            {"Spend($)":(_cat4["spend"],C["spend"]),"Sales($)":(_cat4["sales"],C["sales"])},
            title="키워드 카테고리별 Spend/Sales",height=280)
        st.plotly_chart(fig_cat,use_container_width=True)
    st.divider()

    # ── ④ 키워드 Scatter: ROAS vs Spend ──────────────────────────
    sec("④ 키워드 시각화 (ROAS × Spend Scatter)")
    _kw4_s=_kw4.copy()
    if not _kw4_s.empty:
        _kw4_s["_color"]=_kw4_s["category"].map({"🏷 Brand":"#3B82F6","⚔ Competitor":"#EF4444","🔍 Generic":"#10B981"}).fillna("#94A3B8")
        fig_sc4=go.Figure()
        for cat,color in [("🏷 Brand","#3B82F6"),("⚔ Competitor","#EF4444"),("🔍 Generic","#10B981")]:
            _s=_kw4_s[_kw4_s["category"]==cat]
            if _s.empty: continue
            fig_sc4.add_trace(go.Scatter(
                x=_s["spend"],y=_s["roas"],mode="markers",name=cat,
                marker=dict(size=np.clip(_s["clicks"]/max(_s["clicks"].max(),1)*40+5,5,40),
                            color=color,opacity=0.7),
                text=_s["search_term"],
                hovertemplate="%{text}<br>Spend: $%{x:,.0f}<br>ROAS: %{y:.2f}x<extra></extra>"))
        fig_sc4.add_hline(y=_BE4,line_dash="dash",line_color="#8B5CF6",annotation_text=f"BE ROAS {_BE4:.2f}")
        fig_sc4.update_layout(**{**BASE_LAYOUT,"height":420,"title":"검색어 ROAS × Spend (크기=클릭수)",
                                  "xaxis_title":"Spend ($)","yaxis_title":"ROAS (x)"})
        st.plotly_chart(fig_sc4,use_container_width=True)
    st.divider()

    # ── ⑤ 구조적 적자 탐지 — SKU × Placement ─────────────────────
    sec("⑤ 구조적 적자 탐지 — SKU × 노출위치")
    with st.expander("📖 구조적 적자 탐지는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **목적:** 같은 SKU라도 노출위치별로 수익성이 다릅니다.

    **활용:** 적자인 SKU×위치 조합의 Bid를 낮추거나 해당 위치를 제외하세요.
    """)
    if "placement" in df.columns and df["placement"].notna().any():
        _sp4=agg_kpis(df,["sku","placement"])
        _sp4["structural_loss"]=_sp4.get("be_spend",pd.Series(1,index=_sp4.index))<=0
        _str_loss=_sp4[_sp4["structural_loss"]==True]
        if _str_loss.empty:
            st.success("✅ 구조적 적자 SKU×Placement 조합 없음")
        else:
            st.error(f"⛔ 구조적 적자 {len(_str_loss)}개 조합 — 광고비 0원이어도 원가가 매출 초과")
            _sl_c=["sku","placement","sales","spend","orders","operating_profit","roas"]
            _sl_c=[c for c in _sl_c if c in _str_loss.columns]
            st.dataframe(_str_loss[_sl_c].sort_values("operating_profit"),
                use_container_width=True,hide_index=True)
    st.divider()

    # ===== RESTORED (NEW — Suggested bid 없이 BE CPC 역산 방식) =====
    # ⑤-b  Bid Gap 분석 (BE CPC vs Actual CPC)
    # 원리: be_spend = sales - units × unit_cogs_adj → be_cpc = be_spend / clicks
    #        bid_gap = actual_cpc - be_cpc → 양수면 과다입찰(적자 구조)
    if HAS_COGS:
        sec("⑤-b Bid Gap 분석 (BE CPC vs 실제 CPC)")
        with st.expander("📖 Bid Gap 분석은 어떻게 보나요?", expanded=False):
            st.markdown("""
        **BE CPC:** 손익분기 달성 최대 CPC. 이보다 높으면 적자.

        **Gap = 실제 CPC − BE CPC:** 양수 → 과다입찰, 음수 → 여유 있음.

        **활용:** Gap 큰 캠페인/키워드부터 Bid 인하. 가장 직접적인 이익 개선.
        """)
        st.caption(
            "Suggested bid 데이터 없이도 **원가 기반 BE CPC**를 역산하여 과다입찰 여부를 판단합니다.  \n"
            "`BE CPC = (Sales - Units × unit_cogs_adj) ÷ Clicks` — 흑자를 유지할 수 있는 최대 CPC"
        )

        # 캠페인 × Ad Group 또는 캠페인 단위 집계
        _bid_group_cols = ["campaign", "ad_group"] if ("ad_group" in df.columns and df["ad_group"].notna().any()) else ["campaign"]
        _bg = agg_kpis(df, _bid_group_cols)
        _bg = _bg[_bg["clicks"] >= max(MIN_CLICKS, 5)].copy()

        if not _bg.empty and "be_spend" in _bg.columns:
            _bg["be_cpc"] = np.where(_bg["clicks"] > 0,
                                     _bg["be_spend"] / _bg["clicks"], 0)
            _bg["bid_gap"] = _bg["cpc"] - _bg["be_cpc"]
            _bg["gap_pct"] = np.where(_bg["be_cpc"] > 0,
                                      _bg["bid_gap"] / _bg["be_cpc"] * 100, np.nan)
            _bg["권고"] = _bg["gap_pct"].apply(
                lambda x: "⬆ CPC 과다 (인하)" if x > 20
                else ("✅ 적정" if pd.notna(x) and abs(x) <= 20
                      else ("⬇ 여유 (확장가능)" if pd.notna(x) and x < -20 else "⚠ 구조적적자")))

            # 요약 KPI
            _over_bid = _bg[_bg["gap_pct"] > 20]
            _under_bid = _bg[_bg["gap_pct"] < -20]
            _ok_bid = _bg[(_bg["gap_pct"].abs() <= 20) & _bg["gap_pct"].notna()]

            _bk1, _bk2, _bk3, _bk4 = st.columns(4)
            kpi_card(_bk1, "과다입찰 (인하 필요)", f"{len(_over_bid)}개",
                     f"Spend {fu(float(_over_bid['spend'].sum()))}", color="red")
            kpi_card(_bk2, "적정 CPC", f"{len(_ok_bid)}개",
                     f"Spend {fu(float(_ok_bid['spend'].sum()))}", color="green")
            kpi_card(_bk3, "여유 (확장 가능)", f"{len(_under_bid)}개",
                     f"Spend {fu(float(_under_bid['spend'].sum()))}", color="blue")
            kpi_card(_bk4, "평균 Gap",
                     f"{_bg['gap_pct'].median():.1f}%" if _bg['gap_pct'].notna().any() else "N/A",
                     "양수=과다입찰", color="amber")

            # 테이블
            _bd_cols = _bid_group_cols + ["clicks", "cpc", "be_cpc", "bid_gap", "gap_pct", "권고",
                                          "spend", "sales", "roas", "operating_profit"]
            _bd_cols = [c for c in _bd_cols if c in _bg.columns]
            _bg_disp = _bg[_bd_cols].sort_values("bid_gap", ascending=False).copy()
            for _mc in ["spend", "sales", "operating_profit"]:
                if _mc in _bg_disp.columns: _bg_disp[_mc] = _bg_disp[_mc].map(fu)
            for _cc in ["cpc", "be_cpc", "bid_gap"]:
                if _cc in _bg_disp.columns: _bg_disp[_cc] = _bg_disp[_cc].map(lambda v: f"${v:.3f}" if pd.notna(v) else "—")
            if "gap_pct" in _bg_disp.columns:
                _bg_disp["gap_pct"] = _bg_disp["gap_pct"].map(lambda v: f"{v:+.1f}%" if pd.notna(v) else "—")
            if "roas" in _bg_disp.columns: _bg_disp["roas"] = _bg_disp["roas"].map(fx)

            _rename_bg = {"campaign": "캠페인", "ad_group": "Ad Group", "clicks": "클릭",
                          "cpc": "실제 CPC", "be_cpc": "BE CPC", "bid_gap": "Gap($)",
                          "gap_pct": "Gap(%)", "권고": "권고", "spend": "Spend($)",
                          "sales": "Sales($)", "roas": "ROAS", "operating_profit": "영업이익($)"}
            st.dataframe(_bg_disp.rename(columns=_rename_bg),
                         use_container_width=True, hide_index=True, height=340)

            # Scatter: Actual CPC vs BE CPC
            _bg_sc = _bg[_bg["be_cpc"] > 0].copy()
            if not _bg_sc.empty:
                fig_bid = go.Figure()
                _bid_colors = {"⬆ CPC 과다 (인하)": "#EF4444", "✅ 적정": "#10B981",
                               "⬇ 여유 (확장가능)": "#3B82F6", "⚠ 구조적적자": "#6B7280"}
                for _adv, _aclr in _bid_colors.items():
                    _bsub = _bg_sc[_bg_sc["권고"] == _adv]
                    if _bsub.empty: continue
                    fig_bid.add_trace(go.Scatter(
                        x=_bsub["be_cpc"], y=_bsub["cpc"],
                        mode="markers+text", name=_adv,
                        marker=dict(size=np.clip(_bsub["spend"] / max(_bsub["spend"].max(), 1) * 40 + 6, 6, 40),
                                    color=_aclr, opacity=0.75),
                        text=_bsub[_bid_group_cols[-1]].str[:15],
                        textposition="top center", textfont=dict(size=8),
                        hovertemplate="CPC: $%{y:.3f}<br>BE CPC: $%{x:.3f}<br>Gap: %{customdata:.1f}%<extra></extra>",
                        customdata=_bsub["gap_pct"]))
                # 45° 기준선 (CPC = BE CPC)
                _max_cpc = max(_bg_sc["cpc"].max(), _bg_sc["be_cpc"].max()) * 1.1
                fig_bid.add_trace(go.Scatter(
                    x=[0, _max_cpc], y=[0, _max_cpc],
                    mode="lines", name="CPC = BE CPC (균형선)",
                    line=dict(color="#6B7280", dash="dash", width=1)))
                fig_bid.update_layout(**{**BASE_LAYOUT, "height": 420,
                    "title": "Bid Gap Scatter (45°선 위 = 과다입찰)",
                    "xaxis_title": "BE CPC ($) — 최대 허용 CPC",
                    "yaxis_title": "실제 CPC ($)"})
                st.plotly_chart(fig_bid, use_container_width=True)

            st.download_button("📥 Bid Gap CSV",
                               _bg.to_csv(index=False).encode("utf-8-sig"),
                               "bid_gap_analysis.csv", key="dl_bidgap4")
        else:
            st.info("클릭이 충분한 캠페인/Ad Group이 없거나, be_spend 계산이 불가합니다.")
    st.divider()

    # ── ⑥ 원가 기반 SKU P&L ──────────────────────────────────────
    if HAS_COGS:
        sec("⑥ 원가 기반 SKU 손익 (P&L)")
        with st.expander("📖 SKU 손익은 어떻게 보나요?", expanded=False):
            st.markdown("""
        **구성:** 매출 − 원가(COGS) − 광고비 = 영업이익

        **해석:** 영업이익이 음수인 SKU → Bid 인하 또는 가격 인상 검토.

        **활용:** SKU별 이익률을 비교하여 포트폴리오 최적화.
        """)
        _pg4=agg_kpis(df,["sku","asin"])
        _pg4_c=["sku","asin","spend","sales","units","orders","roas","acos_pct","operating_profit","be_roas","be_acos","structural_loss"]
        _pg4_c=[c for c in _pg4_c if c in _pg4.columns]
        _pg4_d=_pg4[_pg4_c].sort_values("operating_profit").copy()
        def _hl(row):
            if row.get("structural_loss",False): return ["background-color:#FEE2E2"]*len(row)
            return [""]*len(row)
        _pg4_fmt=_pg4_d.copy()
        for _mc in ["spend","sales","operating_profit"]:
            if _mc in _pg4_fmt.columns: _pg4_fmt[_mc]=_pg4_fmt[_mc].map(fu)
        for _rc in ["roas","be_roas"]:
            if _rc in _pg4_fmt.columns: _pg4_fmt[_rc]=_pg4_fmt[_rc].replace(np.inf,999.).map(fx)
        for _pc in ["acos_pct","be_acos"]:
            if _pc in _pg4_fmt.columns: _pg4_fmt[_pc]=_pg4_fmt[_pc].map(lambda v: fp(v) if pd.notna(v) else "—")
        for _ic in ["units","orders"]:
            if _ic in _pg4_fmt.columns: _pg4_fmt[_ic]=_pg4_fmt[_ic].map(fi)
        st.dataframe(_pg4_fmt.style.apply(_hl,axis=1),use_container_width=True,hide_index=True,height=380)
        st.download_button("📥 SKU P&L CSV",_pg4[_pg4_c].to_csv(index=False).encode("utf-8-sig"),"sku_pnl.csv",key="dl_pnl4")

        # ===== RESTORED FROM v3_fixed (Safe Version) =====
        # ⑦ BE ACoS 도달 시뮬레이션 + CPC What-if (all-in 원가 기반)
        st.divider()
        sec("⑦ BE ACoS 도달 시뮬레이션")
        with st.expander("📖 BE ACoS 시뮬레이션은 어떻게 보나요?", expanded=False):
            st.markdown("""
        **목적:** 각 SKU가 BE ACoS에 도달하려면 ACoS를 얼마나 줄여야 하는지

        **해석:** 현재 ACoS < BE ACoS → 이미 흑자(녹색). 높으면 → 그 차이만큼 개선 필요(빨간색).

        **What-If:** CPC 변경 슬라이더로 조정 후 ACoS 변화 시뮬레이션 가능.
        """)
        st.caption("선택한 SKU가 BE ACoS에 도달하려면 얼마나 더 팔아야 하는지 계산")

        _sim_pg4 = _pg4[_pg4["sales"] > 0].copy()
        _sim_pg4["display_id"] = _sim_pg4["sku"].fillna(_sim_pg4["asin"])
        _sim_skus4 = _sim_pg4["display_id"].dropna().tolist()

        if _sim_skus4:
            _sel_sku4 = st.selectbox("SKU/ASIN 선택", _sim_skus4, key="be_sim_sku4")
            _sel4 = _sim_pg4[_sim_pg4["display_id"] == _sel_sku4].iloc[0]
            _s4_sales   = float(_sel4["sales"])
            _s4_spend   = float(_sel4["spend"])
            _s4_acos    = float(_sel4["acos_pct"]) / 100 if _sel4.get("acos_pct", 0) > 0 else 0
            _s4_be_acos = float(_sel4["be_acos"]) / 100 if pd.notna(_sel4.get("be_acos")) and _sel4.get("be_acos", 0) > 0 else 0
            _s4_gap     = _s4_acos - _s4_be_acos
            _s4_profit  = float(_sel4.get("operating_profit", 0))

            _sc41, _sc42, _sc43, _sc44 = st.columns(4)
            kpi_card(_sc41, "ACoS", fp(_s4_acos * 100), color="blue")
            kpi_card(_sc42, "BE ACoS", fp(_s4_be_acos * 100) if _s4_be_acos > 0 else "구조적 적자", color="slate")
            _g4cls = "kpi-bad" if _s4_gap > 0.03 else "kpi-good" if _s4_gap < -0.03 else "kpi-sub"
            kpi_card(_sc43, "Gap", f"{_s4_gap*100:+.1f}%pt",
                     "🔴 적자" if _s4_gap > 0.03 else "🟢 흑자" if _s4_gap < -0.03 else "🟡 중립",
                     sub_cls=_g4cls, color="red" if _s4_gap > 0.03 else "green" if _s4_gap < -0.03 else "amber")
            kpi_card(_sc44, "영업이익", fu(_s4_profit), color="green" if _s4_profit > 0 else "red")

            if _s4_be_acos > 0:
                _be_req_sales4 = _s4_spend / _s4_be_acos
                _be_progress4  = (_s4_sales / _be_req_sales4 * 100) if _be_req_sales4 > 0 else 0
                _be_shortfall4 = max(0, _be_req_sales4 - _s4_sales)

                st.markdown(f"""
| 항목 | 값 | 설명 |
|---|---|---|
| Total Spend | {fu(_s4_spend)} | 현재 기간 총 광고비 |
| BE ACoS | {fp(_s4_be_acos*100)} | 이 이상이면 적자 |
| **BE Required Sales** | **{fu(_be_req_sales4)}** | Spend ÷ BE ACoS = 손익분기 매출 |
| Current Sales | {fu(_s4_sales)} | 현재 실제 매출 |
| **BE Progress** | **{_be_progress4:.1f}%** | 현재 매출 ÷ BE 필요 매출 |
| **부족분** | **{fu(_be_shortfall4)}** | 추가로 필요한 매출 |
""")
                # Progress bar
                _pbar4 = min(_be_progress4, 100)
                _pbar_c4 = "#10B981" if _be_progress4 >= 100 else "#F59E0B" if _be_progress4 >= 80 else "#EF4444"
                st.markdown(
                    f'<div style="background:#1E293B;border-radius:8px;padding:2px;margin:8px 0">'
                    f'<div style="width:{_pbar4:.0f}%;background:{_pbar_c4};'
                    f'border-radius:6px;padding:6px 12px;color:white;font-weight:600;'
                    f'font-size:14px;text-align:right;min-width:60px">'
                    f'{_be_progress4:.1f}%</div></div>', unsafe_allow_html=True)

                if _s4_gap > 0.03:
                    st.error(f"🔴 **위험**: ACoS가 BE보다 {_s4_gap*100:.1f}%pt 초과 → "
                             f"매출 **{fu(_be_shortfall4)}** 추가 필요")
                elif _s4_gap < -0.03:
                    _headroom4 = abs(_s4_gap) * _s4_sales
                    st.success(f"🟢 **스케일 가능**: ACoS 여유 {abs(_s4_gap)*100:.1f}%pt → "
                               f"광고비 **{fu(_headroom4)}** 추가 투입 가능")
                else:
                    st.warning("🟡 **중립**: BE 근접 — 세밀한 CPC 관리 필요")

                # CPC What-if
                st.markdown("##### 🎛 What-If: CPC 변화 → ACoS 영향")
                _cpc_adj4 = st.slider("CPC 변화율 (%)", -50, 50, 0, 5, key="be_cpc_adj4")
                _new_spend4 = _s4_spend * (1 + _cpc_adj4 / 100)
                _new_acos4  = _new_spend4 / _s4_sales if _s4_sales > 0 else 0
                _new_gap4   = _new_acos4 - _s4_be_acos

                _wc41, _wc42, _wc43 = st.columns(3)
                kpi_card(_wc41, "조정 후 Spend", fu(_new_spend4), f"CPC {_cpc_adj4:+d}%", color="blue")
                kpi_card(_wc42, "조정 후 ACoS", fp(_new_acos4 * 100), f"현재 {fp(_s4_acos*100)}", color="amber")
                _ngcls4 = "kpi-bad" if _new_gap4 > 0.03 else "kpi-good" if _new_gap4 < -0.03 else "kpi-sub"
                kpi_card(_wc43, "조정 후 Gap", f"{_new_gap4*100:+.1f}%pt",
                         "🟢 흑자" if _new_gap4 < -0.03 else "🔴 적자" if _new_gap4 > 0.03 else "🟡 중립",
                         sub_cls=_ngcls4,
                         color="green" if _new_gap4 < -0.03 else "red" if _new_gap4 > 0.03 else "amber")
            else:
                st.warning("⚠ BE ACoS = 0 — 구조적 적자 상태입니다. 원가 구조를 점검하세요.")
        else:
            st.info("매출이 있는 SKU가 없습니다.")

    else:
        st.info("ℹ 원가 마스터를 업로드하면 SKU P&L이 활성화됩니다.")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 5 — 판매팀 액션 플랜                                       ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB5:
    _BE5=_BE_ROAS_TOT

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # 📋 실행 액션 플랜 — 우선순위 (이번주 / 1~2주 / 1개월+)
    _cv5_full = agg_kpis(df, ["campaign", "campaign_type"])
    _cv5_full["verdict"] = _cv5_full.apply(lambda r: _apply_verdict(r, _BE5, IS_GROWTH_MODE, TARGET_ACOS), axis=1)

    _cuts5p  = _cv5_full[_cv5_full["verdict"].str.contains("CUT")].sort_values("operating_profit")
    _fixes5p = _cv5_full[_cv5_full["verdict"].str.contains("FIX")].sort_values("operating_profit")
    _scales5p= _cv5_full[_cv5_full["verdict"].str.contains("SCALE")].sort_values("operating_profit", ascending=False)

    _neg5p = agg_kpis(df, ["search_term", "campaign"])
    _neg5p = _neg5p[(_neg5p["clicks"] >= MIN_CLICKS) & (_neg5p["orders"] == 0)]
    _neg_waste5p = float(_neg5p["spend"].sum())

    sec("📋 실행 액션 플랜 — 우선순위")
    with st.expander("📖 액션 플랜은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **우선순위:**
    1. **즉시 효과:** 적자 Pause, Negative 등록 → 당장 비용 절감
    2. **단기 개선:** FIX Bid 조정, Auto→Manual → 1~2주 내 효과
    3. **중기 성장:** SCALE 확대, 리스팅 최적화 → 2~4주 소요
    4. **장기 투자:** 신규 키워드, 브랜드 캠페인 → 1개월+

    **핵심:** 1순위부터 순서대로 실행. 상위 액션이 하위 효과를 높여줍니다.
    """)

    st.markdown("### 🚨 즉시 실행 (이번 주)")
    if not _cuts5p.empty:
        _cut_spend5p = float(_cuts5p["spend"].sum())
        _cut_loss5p  = float(_cuts5p["operating_profit"].sum())
        st.markdown(f"""
**1️⃣ 적자 캠페인 Pause/Bid 인하** — {len(_cuts5p)}개 캠페인
- 합산 Spend: {fu(_cut_spend5p)} → 합산 손실: {fu(_cut_loss5p)}
- **액션**: 즉시 Pause 또는 Bid 50% 인하
- **예상 효과**: 월 {fu(abs(_cut_loss5p))} 손실 방지
""")
        _cut_d5p = _cuts5p[["campaign", "spend", "sales", "roas", "operating_profit"]].copy()
        for c in ["spend", "sales", "operating_profit"]: _cut_d5p[c] = _cut_d5p[c].map(fu)
        _cut_d5p["roas"] = _cut_d5p["roas"].map(fx)
        st.dataframe(_cut_d5p.rename(columns={"campaign": "캠페인",
                     "spend": "Spend", "sales": "Sales", "roas": "ROAS", "operating_profit": "영업이익"}),
            use_container_width=True, hide_index=True)
    else:
        st.success("✅ CUT 대상 캠페인 없음")

    if not _neg5p.empty:
        st.markdown(f"""
**2️⃣ Negative 키워드 등록** — {len(_neg5p)}개 검색어
- 클릭 {MIN_CLICKS}+ 전환 0 → 낭비 {fu(_neg_waste5p)}
- **액션**: 검색어 리스트 다운로드 → Negative Exact로 등록
""")

    st.markdown("---")
    st.markdown("### 🔧 단기 최적화 (1~2주)")
    if not _fixes5p.empty:
        st.markdown(f"""
**3️⃣ FIX 캠페인 효율 개선** — {len(_fixes5p)}개 캠페인
- BE ROAS {fx(_BE5)} 근접이나 미달 → Bid/타겟 최적화로 전환
""")
        _fix_d5p = _fixes5p[["campaign", "spend", "sales", "roas", "operating_profit"]].copy()
        for c in ["spend", "sales", "operating_profit"]: _fix_d5p[c] = _fix_d5p[c].map(fu)
        _fix_d5p["roas"] = _fix_d5p["roas"].map(fx)
        st.dataframe(_fix_d5p.rename(columns={"campaign": "캠페인",
                     "spend": "Spend", "sales": "Sales", "roas": "ROAS", "operating_profit": "영업이익"}),
            use_container_width=True, hide_index=True)

    # Auto → Manual 프로모션
    if df["campaign_type"].str.contains("Auto").any():
        _auto_st5p = agg_kpis(df[df["campaign_type"] == "Auto"], ["search_term"])
        _promo5p = _auto_st5p[(_auto_st5p["orders"] >= 2) & (_auto_st5p["roas"] >= _BE5)]
        if not _promo5p.empty:
            st.markdown(f"""
**4️⃣ Auto → Manual 프로모션** — {len(_promo5p)}개 검색어
- Auto 캠페인에서 주문 2+ & ROAS ≥ BE인 검색어 → Manual Exact로 이관
""")

    st.markdown("---")
    st.markdown("### 📈 중기 전략 (1개월+)")
    if not _scales5p.empty:
        st.markdown(f"""
**5️⃣ SCALE 캠페인 예산 확대** — {len(_scales5p)}개 캠페인
- ROAS가 BE 대비 20%+ → 예산 증액으로 매출 확대
""")
        _scale_d5p = _scales5p[["campaign", "spend", "sales", "roas", "operating_profit"]].copy()
        for c in ["spend", "sales", "operating_profit"]: _scale_d5p[c] = _scale_d5p[c].map(fu)
        _scale_d5p["roas"] = _scale_d5p["roas"].map(fx)
        st.dataframe(_scale_d5p.rename(columns={"campaign": "캠페인",
                     "spend": "Spend", "sales": "Sales", "roas": "ROAS", "operating_profit": "영업이익"}),
            use_container_width=True, hide_index=True)

    st.divider()

    # ── 캠페인 Verdict 테이블 ─────────────────────────────────────
    sec("📋 캠페인 Verdict (CUT / FIX / HOLD / SCALE)")
    with st.expander("📖 캠페인 Verdict는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **수익 모드:** ROAS vs BE — SCALE(≥1.2x) / HOLD(≥0.9x) / FIX(≥0.7x) / CUT(<0.7x)
    **성장 모드:** ACoS vs 목표 — SCALE(≤목표) / HOLD(≤1.3x) / FIX(≤2x) / CUT(>2x)

    **활용:** CUT 정리 → FIX 최적화 → SCALE 확대 순서.
    """)
    _cv=agg_kpis(df,["campaign","campaign_type"])
    _cv["verdict"]=_cv.apply(lambda r:_apply_verdict(r,_BE5,IS_GROWTH_MODE,TARGET_ACOS),axis=1)
    _cv["action"]=_cv["verdict"].map({
        "🟢 SCALE":"예산 +20~50% 확대 검토",
        "🟡 HOLD":"현 수준 유지, 주간 모니터링",
        "🔧 FIX":"Bid 인하 + Negative 추가",
        "🔴 CUT":"Pause 또는 예산 0으로 감소"})
    _cv_cols=["campaign","campaign_type","spend","sales","roas","acos_pct","operating_profit","verdict","action"]
    _cv_cols=[c for c in _cv_cols if c in _cv.columns]
    _cv_d=_cv[_cv_cols].sort_values("operating_profit").copy()
    _verdict_colors={"🟢 SCALE":"#DCFCE7","🟡 HOLD":"#FEF9C3","🔧 FIX":"#DBEAFE","🔴 CUT":"#FEE2E2"}
    def _cv_hl(row):
        bg=_verdict_colors.get(row.get("verdict",""),""  )
        return [f"background-color:{bg}" if bg else ""]*len(row)
    _cv_fmt=_cv_d.copy()
    for _mc in ["spend","sales","operating_profit"]: _cv_fmt[_mc]=_cv_fmt[_mc].map(fu)
    _cv_fmt["roas"]=_cv_fmt["roas"].map(fx); _cv_fmt["acos_pct"]=_cv_fmt["acos_pct"].map(fp)
    st.dataframe(_cv_fmt.rename(columns={"campaign":"캠페인","campaign_type":"유형","spend":"Spend($)",
        "sales":"Sales($)","roas":"ROAS","acos_pct":"ACoS(%)","operating_profit":"영업이익($)",
        "verdict":"Verdict","action":"권고 액션"}).style.apply(_cv_hl,axis=1),
        use_container_width=True,hide_index=True,height=380)
    _scale=_cv[_cv["verdict"]=="🟢 SCALE"]; _cut=_cv[_cv["verdict"]=="🔴 CUT"]
    _c5a,_c5b=st.columns(2)
    with _c5a: st.success(f"🟢 SCALE: {len(_scale)}개 캠페인 · Spend {fu(float(_scale['spend'].sum()))}")
    with _c5b: st.error(f"🔴 CUT: {len(_cut)}개 캠페인 · 손실 {fu(abs(float(_cut['operating_profit'].sum())))}")
    st.download_button("📥 액션 플랜 CSV",_cv[_cv_cols].to_csv(index=False).encode("utf-8-sig"),"action_plan.csv",key="dl_action5")
    st.divider()

    # ── Negative 키워드 후보 ──────────────────────────────────────
    sec("🚫 Negative 키워드 후보")
    with st.expander("📖 Negative 키워드는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **선정 기준:** 클릭 있지만 주문 0인 검색어 (돈만 쓰고 전환 없음)

    **등록:** CSV 다운로드 → Seller Central → Negative Keywords에 Exact 매치로 추가.

    **주의:** 클릭 5 미만은 데이터 부족. 더 쌓인 후 재평가.
    """)
    _neg5=agg_kpis(df,["search_term","campaign"])
    _neg5=_neg5[(_neg5["clicks"]>=MIN_CLICKS)&(_neg5["orders"]==0)]
    if _neg5.empty:
        st.success("Negative 등록 대상 없음")
    else:
        st.error(f"🚨 {len(_neg5)}개 검색어 — 낭비 추정 {fu(float(_neg5['spend'].sum()))}")
        _n5c=["search_term","campaign","clicks","spend","impressions","ctr_pct"]
        _n5c=[c for c in _n5c if c in _neg5.columns]
        st.dataframe(_neg5[_n5c].sort_values("spend",ascending=False).rename(columns={
            "search_term":"검색어","campaign":"캠페인","clicks":"클릭",
            "spend":"낭비($)","impressions":"노출","ctr_pct":"CTR(%)"}),
            use_container_width=True,hide_index=True,height=280)
        st.download_button("📥 Negative 후보 CSV",_neg5[["search_term","campaign","clicks","spend"]].to_csv(index=False).encode("utf-8-sig"),"neg_keywords.csv",key="dl_neg5")
    st.divider()

    # ── Auto → Manual 전환 후보 ───────────────────────────────────
    sec("🔄 Auto → Manual 전환 후보 (성과 좋은 Auto 키워드)")
    with st.expander("📖 Auto→Manual 전환은 어떻게 보나요?", expanded=False):
        st.markdown("""
    **선정 기준:** Auto에서 주문 2건+ & ACoS ≤ 목표인 검색어

    **이유:** Manual Exact로 이관 시 CPC 직접 제어 가능 (15~20% 절감). CVR도 개선.

    **방법:** Manual Exact에 추가 + Auto에서 Negative 등록.
    """)
    _auto5=df[df["campaign_type"]=="Auto"].copy()
    if _auto5.empty:
        st.info("Auto 캠페인 데이터 없음")
    else:
        _aq5=agg_kpis(_auto5,["search_term","campaign"])
        _promo=_aq5[(_aq5["orders"]>=2)&(_aq5["acos_pct"]<=TARGET_ACOS)&(_aq5["clicks"]>=MIN_CLICKS)]
        if _promo.empty:
            st.info(f"전환 후보 없음 (기준: 주문≥2 + ACoS≤{TARGET_ACOS:.0f}% + 클릭≥{MIN_CLICKS})")
        else:
            st.success(f"✅ {len(_promo)}개 검색어 — Manual Exact 등록 추천")
            _pc5=["search_term","campaign","orders","clicks","spend","sales","roas","acos_pct","cvr_pct"]
            _pc5=[c for c in _pc5 if c in _promo.columns]
            st.dataframe(_promo[_pc5].sort_values("orders",ascending=False).rename(columns={
                "search_term":"검색어","campaign":"캠페인","orders":"주문","clicks":"클릭",
                "spend":"Spend($)","sales":"Sales($)","roas":"ROAS","acos_pct":"ACoS(%)","cvr_pct":"CVR(%)"}),
                use_container_width=True,hide_index=True,height=280)
            st.download_button("📥 Manual 전환 후보 CSV",_promo[["search_term","campaign","orders","spend","acos_pct"]].to_csv(index=False).encode("utf-8-sig"),"manual_promo.csv",key="dl_promo5")
    st.divider()

    # ── [v3.2 복원] 📎 캠페인 목적별 분류 (자동) ─────────────────
    sec("📎 캠페인 목적별 분류")
    with st.expander("📖 캠페인 목적별 분류는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **자동 분류:** 캠페인 이름으로 Brand/Competitor/Generic/Auto 추정

    **활용:** 목적별 효율 비교 → 어떤 유형에 더 투자해야 하는지 판단.
    """)
    _cv_purpose = _cv.copy()
    _cv_purpose["목적"] = _cv_purpose["campaign"].apply(_classify_purpose)
    _pg5 = _cv_purpose.groupby("목적").agg(
        캠페인수=("campaign", "count"),
        Spend=("spend", "sum"), Sales=("sales", "sum"), Orders=("orders", "sum"),
        이익=("operating_profit", "sum")
    ).reset_index()
    _pg5["ROAS"] = _pg5["Sales"] / _pg5["Spend"].replace(0, np.nan)
    _pg5 = _pg5.sort_values("Spend", ascending=False)

    _fig_pg5 = make_subplots(specs=[[{"secondary_y": True}]])
    _fig_pg5.add_trace(go.Bar(x=_pg5["목적"], y=_pg5["Spend"], name="Spend",
                               marker_color=C["spend"]), secondary_y=False)
    _fig_pg5.add_trace(go.Bar(x=_pg5["목적"], y=_pg5["Sales"], name="Sales",
                               marker_color=C["sales"]), secondary_y=False)
    _fig_pg5.add_trace(go.Scatter(x=_pg5["목적"], y=_pg5["ROAS"], name="ROAS",
                                   mode="markers+text", text=_pg5["ROAS"].map(fx),
                                   textposition="top center",
                                   marker=dict(size=14, color=C["roas"])),
                        secondary_y=True)
    if _BE_ROAS_TOT < 900:
        _fig_pg5.add_hline(y=_BE_ROAS_TOT, line_dash="dash", line_color="#EF4444",
                           annotation_text=f"BE {fx(_BE_ROAS_TOT)}", secondary_y=True)
    _fig_pg5.update_layout(**{**BASE_LAYOUT, "barmode": "group", "height": 320,
                              "title": "캠페인 목적별 ROI 비교"})
    st.plotly_chart(_fig_pg5, use_container_width=True)

    _pg5d = _pg5.copy()
    for c in ["Spend", "Sales", "이익"]: _pg5d[c] = _pg5d[c].map(fu)
    _pg5d["ROAS"] = _pg5d["ROAS"].map(fx)
    st.dataframe(_pg5d, use_container_width=True, hide_index=True)
    st.divider()

    # ── [v3.2 복원] 📊 Verdict 분포 (파이차트) ───────────────────
    sec("📊 Verdict 분포")
    with st.expander("📖 Verdict 분포는 어떻게 보나요?", expanded=False):
        st.markdown("""
    **건강한 분포:** SCALE+HOLD ≥ 60% → 대부분 효율적
    **위험한 분포:** CUT+FIX ≥ 50% → 구조적 비효율, 대대적 정리 필요

    **Spend 기준으로도 확인:** CUT에 Spend가 집중되면 더 심각.
    """)
    _vd_summary = _cv.groupby("verdict").agg(개수=("campaign","count"),
                  Spend합=("spend","sum")).reset_index()
    _vd_colors5 = {"🟢 SCALE": "#10B981", "🟡 HOLD": "#F59E0B",
                   "🔧 FIX": "#3B82F6", "🔴 CUT": "#EF4444"}
    _fig_vd5 = go.Figure(go.Pie(
        labels=_vd_summary["verdict"], values=_vd_summary["개수"],
        marker=dict(colors=[_vd_colors5.get(v, "#94A3B8") for v in _vd_summary["verdict"]]),
        hole=0.4, textinfo="label+value+percent"))
    _fig_vd5.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(_fig_vd5, use_container_width=True)
    st.divider()

    # ── [v3.2 복원] ✅ 실행 체크리스트 ────────────────────────────
    sec("✅ 실행 체크리스트 — 적용 시뮬레이션")
    with st.expander("📖 이 시뮬레이터는 어떻게 보나요?", expanded=False):
        st.markdown("""
**목적:** 판매팀이 실행할 수 있는 액션들을 각각 토글하여, 적용 전/후 전체 성과 변화를 즉시 확인

**각 액션의 가정:**
- 🔴 **CUT (Pause):** 적자 캠페인의 Spend와 Sales가 모두 0이 됨. 영업이익은 적자 만큼 개선
- 🚫 **Negative:** 전환 없는 검색어의 Spend가 100% 절감됨
- 🔧 **FIX:** ACoS가 15% 개선된다고 가정 (Bid 최적화, 타겟 정리)
- 🟢 **SCALE:** 예산 15% 증액, 기존 ROAS 유지 가정
- 🔄 **Auto→Manual:** CPC 15% 절감, CVR 10% 개선 가정
- 📸 **Low CTR 개선:** CTR이 0.3%까지 개선, 기존 CVR 유지
- 📉 **High ACoS Bid↓:** CPC 20% 절감, 노출 10% 감소 가정

**핵심:** 여러 액션을 동시에 토글하면 복합 효과를 볼 수 있습니다.
""")
    _cuts5 = _cv[_cv["verdict"] == "🔴 CUT"]
    _fixes5 = _cv[_cv["verdict"] == "🔧 FIX"]
    _scales5 = _cv[_cv["verdict"] == "🟢 SCALE"]
    _neg_waste5 = float(_neg5["spend"].sum()) if not _neg5.empty else 0.0

    _cut_loss5 = abs(float(_cuts5["operating_profit"].sum())) if not _cuts5.empty else 0
    _cut_spend5 = float(_cuts5["spend"].sum()) if not _cuts5.empty else 0
    _fix_potential5 = float(_fixes5["spend"].sum()) * 0.15 if not _fixes5.empty else 0  # 15% 효율 개선 가정
    _scale_spend5 = float(_scales5["spend"].sum()) if not _scales5.empty else 0
    _scale_sales5 = float(_scales5["sales"].sum()) if not _scales5.empty else 0

    st.caption("각 액션을 토글하면 적용 전/후 예상 성과가 자동 계산됩니다.")

    # 토글들
    _do_cut = st.checkbox(
        f"🔴 적자 캠페인 {len(_cuts5)}개 Pause — 예상 절감 {fu(_cut_loss5)}",
        value=False, key="sim_cut5") if not _cuts5.empty else False
    _do_neg = st.checkbox(
        f"🚫 Negative 키워드 {len(_neg5)}개 등록 — 예상 절감 {fu(_neg_waste5)}/월",
        value=False, key="sim_neg5") if _neg_waste5 > 0 else False
    _do_fix = st.checkbox(
        f"🔧 FIX 캠페인 {len(_fixes5)}개 효율 개선 (ACoS 15% 개선 가정)",
        value=False, key="sim_fix5") if not _fixes5.empty else False
    _do_scale = st.checkbox(
        f"🟢 SCALE 캠페인 {len(_scales5)}개 예산 15% 증액",
        value=False, key="sim_scale5") if not _scales5.empty else False

    # Auto → Manual 전환 후보
    _auto5_check = df[df["campaign_type"]=="Auto"].copy()
    _promo5_check = pd.DataFrame()
    if not _auto5_check.empty:
        _aq5_check = agg_kpis(_auto5_check, ["search_term", "campaign"])
        _promo5_check = _aq5_check[(_aq5_check["orders"]>=2)&(_aq5_check["acos_pct"]<=TARGET_ACOS)&(_aq5_check["clicks"]>=MIN_CLICKS)]
    _do_auto = st.checkbox(
        f"🔄 Auto→Manual 전환 {len(_promo5_check)}개 검색어 — CPC 절감 + CVR 개선 기대",
        value=False, key="sim_auto5") if not _promo5_check.empty else False

    # Low CTR 캠페인 소재 개선
    _low_ctr5 = _cv[(_cv["ctr_pct"] < 0.3) & (_cv["impressions"] > 100)] if "ctr_pct" in _cv.columns else pd.DataFrame()
    _low_ctr_spend5 = float(_low_ctr5["spend"].sum()) if not _low_ctr5.empty else 0
    _do_ctr = st.checkbox(
        f"📸 Low CTR 캠페인 {len(_low_ctr5)}개 소재 개선 — CTR 0.3% 미만, Spend {fu(_low_ctr_spend5)}",
        value=False, key="sim_ctr5") if not _low_ctr5.empty else False

    # High ACoS 캠페인 Bid 인하
    _high_acos5 = _cv[(_cv["acos_pct"] > TARGET_ACOS * 2) & (_cv["spend"] > 50)] if "acos_pct" in _cv.columns else pd.DataFrame()
    _high_acos_spend5 = float(_high_acos5["spend"].sum()) if not _high_acos5.empty else 0
    _do_acos = st.checkbox(
        f"📉 High ACoS 캠페인 {len(_high_acos5)}개 Bid 20% 인하 — ACoS>{TARGET_ACOS*2:.0f}%, Spend {fu(_high_acos_spend5)}",
        value=False, key="sim_acos5") if not _high_acos5.empty else False

    # 시뮬레이션 계산
    _sim5_spend = _SPEND
    _sim5_sales = _SALES
    _sim5_profit = _PROFIT
    _sim5_actions = []

    if _do_cut:
        _sim5_spend -= _cut_spend5
        _sim5_sales -= float(_cuts5["sales"].sum())
        _sim5_profit += _cut_loss5
        _sim5_actions.append(f"CUT: Spend -{fu(_cut_spend5)}, 손실 방지 +{fu(_cut_loss5)}")

    if _do_neg:
        _sim5_spend -= _neg_waste5
        _sim5_profit += _neg_waste5
        _sim5_actions.append(f"NEG: 낭비 제거 -{fu(_neg_waste5)}")

    if _do_fix:
        _fix_sales_gain = float(_fixes5["sales"].sum()) * 0.15
        _sim5_sales += _fix_sales_gain
        _sim5_profit += _fix_sales_gain
        _sim5_actions.append(f"FIX: Sales +{fu(_fix_sales_gain)} (15% 효율 개선)")

    if _do_scale:
        _scale_add_spend = _scale_spend5 * 0.15
        _scale_roas_avg = _scale_sales5 / _scale_spend5 if _scale_spend5 > 0 else _ROAS
        _scale_add_sales = _scale_add_spend * _scale_roas_avg
        _sim5_spend += _scale_add_spend
        _sim5_sales += _scale_add_sales
        _sim5_profit += _scale_add_sales - _scale_add_spend
        _sim5_actions.append(f"SCALE: Spend +{fu(_scale_add_spend)}, Sales +{fu(_scale_add_sales)}")

    if _do_auto:
        # Auto→Manual: CPC 15% 절감 + CVR 10% 개선 가정
        _auto_spend = float(_promo5_check["spend"].sum())
        _auto_saving = _auto_spend * 0.15
        _auto_sales_gain = float(_promo5_check["sales"].sum()) * 0.10
        _sim5_spend -= _auto_saving
        _sim5_sales += _auto_sales_gain
        _sim5_profit += _auto_saving + _auto_sales_gain
        _sim5_actions.append(f"AUTO→MANUAL: CPC 절감 {fu(_auto_saving)} + Sales +{fu(_auto_sales_gain)}")

    if _do_ctr:
        # Low CTR 소재 개선: CTR 2배 → 클릭 2배 → 동일 CVR로 주문 증가
        _ctr_impr = float(_low_ctr5["impressions"].sum()) if not _low_ctr5.empty else 0
        _ctr_extra_clicks = _ctr_impr * 0.003  # 0.3%까지 CTR 개선
        _ctr_cvr_avg = float(_low_ctr5["cvr_pct"].mean()) / 100 if not _low_ctr5.empty and "cvr_pct" in _low_ctr5.columns else 0.08
        _ctr_extra_orders = _ctr_extra_clicks * _ctr_cvr_avg
        _ctr_extra_sales = _ctr_extra_orders * _AOV
        _ctr_extra_spend = _ctr_extra_clicks * _CPC
        _sim5_spend += _ctr_extra_spend
        _sim5_sales += _ctr_extra_sales
        _sim5_profit += _ctr_extra_sales - _ctr_extra_spend
        _sim5_actions.append(f"CTR 개선: 추가 클릭 +{fi(int(_ctr_extra_clicks))}, Sales +{fu(_ctr_extra_sales)}")

    if _do_acos:
        # High ACoS Bid 20% 인하: CPC -20% → Spend -20%, 노출 -10%(탄력성 0.5)
        _hacos_spend_save = _high_acos_spend5 * 0.20
        _hacos_sales_loss = float(_high_acos5["sales"].sum()) * 0.10  # 노출 감소로 매출 10% 감소
        _sim5_spend -= _hacos_spend_save
        _sim5_sales -= _hacos_sales_loss
        _sim5_profit += _hacos_spend_save - _hacos_sales_loss
        _sim5_actions.append(f"HIGH ACoS Bid↓: Spend -{fu(_hacos_spend_save)}, Sales -{fu(_hacos_sales_loss)}")

    _any_action = _do_cut or _do_neg or _do_fix or _do_scale or _do_auto or _do_ctr or _do_acos

    if _any_action:
        st.divider()
        st.markdown("##### 📊 적용 전 → 후 비교")
        _sim5_roas = _sim5_sales / _sim5_spend if _sim5_spend > 0 else 0
        _sim5_acos = _sim5_spend / _sim5_sales * 100 if _sim5_sales > 0 else 999

        _bf_c1, _bf_c2, _bf_c3, _bf_c4 = st.columns(4)
        _bf_c1.metric("Spend", fu(_sim5_spend), f"{_sim5_spend - _SPEND:+,.0f}", delta_color="inverse")
        _bf_c2.metric("Sales", fu(_sim5_sales), f"{_sim5_sales - _SALES:+,.0f}", delta_color="normal")
        _bf_c3.metric("영업이익", fu(_sim5_profit), f"{_sim5_profit - _PROFIT:+,.0f}",
                       delta_color="normal" if _sim5_profit > _PROFIT else "inverse")
        _bf_c4.metric("ROAS", f"{_sim5_roas:.2f}x", f"{_sim5_roas - _ROAS:+.2f}x",
                       delta_color="normal")

        _compare5 = pd.DataFrame([
            {"지표": "Spend ($)", "현재": fu(_SPEND), "적용 후": fu(_sim5_spend), "변화": f"{(_sim5_spend-_SPEND)/_SPEND*100:+.1f}%" if _SPEND > 0 else "—"},
            {"지표": "Sales ($)", "현재": fu(_SALES), "적용 후": fu(_sim5_sales), "변화": f"{(_sim5_sales-_SALES)/_SALES*100:+.1f}%" if _SALES > 0 else "—"},
            {"지표": "ROAS (x)", "현재": fx(_ROAS), "적용 후": f"{_sim5_roas:.2f}x", "변화": f"{_sim5_roas-_ROAS:+.2f}x"},
            {"지표": "ACoS (%)", "현재": fp(_ACOS), "적용 후": f"{_sim5_acos:.1f}%", "변화": f"{_sim5_acos-_ACOS:+.1f}%p"},
            {"지표": "영업이익 ($)", "현재": fu(_PROFIT), "적용 후": fu(_sim5_profit), "변화": fu(_sim5_profit - _PROFIT)},
        ])
        st.dataframe(_compare5, use_container_width=True, hide_index=True)

        # 적용 액션 요약
        st.markdown("**📋 적용된 액션:**")
        for a in _sim5_actions:
            st.markdown(f"  • {a}")

        _total_improvement = _sim5_profit - _PROFIT
        if _total_improvement > 0:
            st.success(f"✅ 총 예상 개선 효과: **{fu(_total_improvement)}**/월")
        else:
            st.warning(f"⚠️ 순 효과: {fu(_total_improvement)}/월 — 일부 액션의 비용이 절감보다 큽니다.")
    else:
        # 기존 체크리스트 표시 (토글 안 했을 때)
        _total_potential5 = _cut_loss5 + _neg_waste5
        if _total_potential5 > 0:
            st.info(f"👆 위 항목을 토글하면 적용 전/후 예상 성과를 비교할 수 있습니다.  \n"
                    f"**전체 적용 시 예상 개선: {fu(_total_potential5)}+/월**")
        else:
            st.success("✅ 모든 캠페인이 양호 — 현행 유지 + SCALE 검토")

    st.download_button("📥 전체 캠페인 Verdict CSV",
                       _cv.to_csv(index=False).encode("utf-8-sig"),
                       "campaign_verdicts.csv", key="dl_verdicts5")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 6 — AI & ML Lab                                            ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB6:
    st.caption("머신러닝 분석 · 통계 검증 · 데이터 품질 관리")

    # ML 데이터 준비
    _ai_camp=agg_kpis(df,["campaign","campaign_type"])
    _ai_camp["profit_proxy"]=_ai_camp["operating_profit"]
    _ml_ready = len(_ai_camp) >= 5

    _t6a,_t6b,_t6c,_t6d,_t6e,_t6f = st.tabs([
        "🌲 Feature Importance","📊 K-Means Clustering",
        "🌳 Decision Tree","🔥 Correlation","📐 통계 Lab","🔬 Forecast (WIP)"])

    with _t6a:
        sec("🌲 Random Forest Feature Importance")
        if not _ml_ready:
            st.info(f"ML 분석에는 캠페인 5개 이상 필요합니다. (현재: {len(_ai_camp)}개)")
        else:
            try:
                from sklearn.ensemble import RandomForestRegressor
                from sklearn.preprocessing import StandardScaler
                _feats=["spend","roas","ctr_pct","cvr_pct","cpc","aov","impressions","clicks"]
                _feats=[c for c in _feats if c in _ai_camp.columns]
                _target="operating_profit"
                _ml_df=_ai_camp[_feats+[_target]].dropna()
                if len(_ml_df)<5:
                    st.info("유효 데이터 부족")
                else:
                    _X=_ml_df[_feats].values; _y=_ml_df[_target].values
                    _sc=StandardScaler(); _Xs=_sc.fit_transform(_X)
                    _rf=RandomForestRegressor(n_estimators=100,random_state=42); _rf.fit(_Xs,_y)
                    _imp=pd.DataFrame({"Feature":_feats,"Importance":_rf.feature_importances_}).sort_values("Importance",ascending=True)
                    fig_imp=go.Figure(go.Bar(x=_imp["Importance"],y=_imp["Feature"],orientation="h",
                                             marker_color="#3B82F6",text=_imp["Importance"].map(lambda v:f"{v:.3f}"),
                                             textposition="outside"))
                    fig_imp.update_layout(**{**BASE_LAYOUT,"height":360,"title":"Feature Importance (영업이익 예측)",
                                             "xaxis_title":"Importance","margin":dict(l=120,r=80,t=40,b=4)})
                    st.plotly_chart(fig_imp,use_container_width=True)
                    st.caption(f"R² (train): {_rf.score(_Xs,_y):.3f} · 캠페인수: {len(_ml_df)}")
            except ImportError:
                st.warning("scikit-learn이 설치되어 있지 않습니다. `pip install scikit-learn`")

    with _t6b:
        sec("📊 K-Means Clustering (캠페인 군집화)")
        if not _ml_ready:
            st.info("ML 분석에는 캠페인 5개 이상 필요합니다.")
        else:
            try:
                from sklearn.cluster import KMeans
                from sklearn.preprocessing import StandardScaler
                _k_feats=["roas","acos_pct","cpc","cvr_pct","spend"]
                _k_feats=[c for c in _k_feats if c in _ai_camp.columns]
                _kdf=_ai_camp[_k_feats+["campaign"]].dropna()
                if len(_kdf)<5:
                    st.info("유효 데이터 부족")
                else:
                    _n_clusters=st.slider("클러스터 수(K)",2,min(6,len(_kdf)),3,key="t6_k")
                    _Xk=StandardScaler().fit_transform(_kdf[_k_feats])
                    _km=KMeans(n_clusters=_n_clusters,random_state=42,n_init=10); _km.fit(_Xk)
                    _kdf["cluster"]=_km.labels_.astype(str)
                    fig_km=go.Figure()
                    _colors_km=["#3B82F6","#10B981","#F59E0B","#EF4444","#8B5CF6","#14B8A6"]
                    for _cl in sorted(_kdf["cluster"].unique()):
                        _sub=_kdf[_kdf["cluster"]==_cl]
                        fig_km.add_trace(go.Scatter(x=_sub["roas"],y=_sub["acos_pct"],
                            mode="markers+text",name=f"Cluster {_cl}",
                            marker=dict(size=14,color=_colors_km[int(_cl)%len(_colors_km)],opacity=0.8),
                            text=_sub["campaign"].str[:12],textposition="top center",textfont=dict(size=8)))
                    fig_km.add_hline(y=TARGET_ACOS,line_dash="dash",line_color="#EF4444")
                    fig_km.add_vline(x=_BE_ROAS_TOT,line_dash="dash",line_color="#8B5CF6")
                    fig_km.update_layout(**{**BASE_LAYOUT,"height":440,"title":"캠페인 K-Means 군집화",
                                            "xaxis_title":"ROAS","yaxis_title":"ACoS(%)"})
                    st.plotly_chart(fig_km,use_container_width=True)
                    # Cluster 요약
                    _ksumm=_kdf.groupby("cluster")[_k_feats].mean().round(2).reset_index()
                    st.dataframe(_ksumm,use_container_width=True,hide_index=True)
            except ImportError:
                st.warning("scikit-learn이 설치되어 있지 않습니다.")

    with _t6c:
        sec("🌳 Decision Tree — 성과 분류")
        if not _ml_ready:
            st.info("ML 분석에는 캠페인 5개 이상 필요합니다.")
        else:
            try:
                from sklearn.tree import DecisionTreeClassifier, export_text
                _dt_feats=["spend","roas","cpc","cvr_pct","impressions"]
                _dt_feats=[c for c in _dt_feats if c in _ai_camp.columns]
                _ai_camp["_label"]=(_ai_camp["operating_profit"]>0).astype(int)
                _dtdf=_ai_camp[_dt_feats+["_label","campaign"]].dropna()
                if len(_dtdf)<5:
                    st.info("유효 데이터 부족")
                else:
                    _max_d=st.slider("트리 깊이",1,5,3,key="t6_dt")
                    _dt=DecisionTreeClassifier(max_depth=_max_d,random_state=42)
                    _dt.fit(_dtdf[_dt_feats],_dtdf["_label"])
                    _dtdf["pred"]=_dt.predict(_dtdf[_dt_feats])
                    _acc=(_dtdf["pred"]==_dtdf["_label"]).mean()
                    st.metric("정확도 (Accuracy)",f"{_acc:.1%}")
                    _tree_txt=export_text(_dt,feature_names=_dt_feats)
                    with st.expander("🌳 트리 규칙 보기"):
                        st.code(_tree_txt)
                    _dtdf["결과"]=_dtdf.apply(lambda r:"✅ 정확" if r["pred"]==r["_label"] else "❌ 오분류",axis=1)
                    _dtdf["예측"]=_dtdf["pred"].map({1:"흑자(예측)",0:"적자(예측)"})
                    _dtdf["실제"]=_dtdf["_label"].map({1:"흑자(실제)",0:"적자(실제)"})
                    st.dataframe(_dtdf[["campaign","예측","실제","결과"]],use_container_width=True,hide_index=True)
            except ImportError:
                st.warning("scikit-learn이 설치되어 있지 않습니다.")

    with _t6d:
        sec("🔥 Correlation Heatmap")
        _corr_feats=["spend","sales","roas","acos_pct","cpc","ctr_pct","cvr_pct","aov","operating_profit","orders"]
        _corr_feats=[c for c in _corr_feats if c in _ai_camp.columns]
        _corr_df=_ai_camp[_corr_feats].dropna()
        if len(_corr_df)>=3:
            _corr_mat=_corr_df.corr()
            fig_corr=go.Figure(go.Heatmap(
                z=_corr_mat.values,x=_corr_mat.columns,y=_corr_mat.columns,
                colorscale="RdBu",zmid=0,zmin=-1,zmax=1,
                text=_corr_mat.values.round(2),texttemplate="%{text}",textfont=dict(size=9),
                colorbar=dict(title="Pearson r")))
            fig_corr.update_layout(**{**BASE_LAYOUT,"height":500,"title":"캠페인 지표 상관관계 Heatmap","margin":dict(l=100,r=40,t=60,b=100)})
            st.plotly_chart(fig_corr,use_container_width=True)
            # 주요 상관관계 해석
            _corr_op=_corr_mat.get("operating_profit",pd.Series()).drop("operating_profit",errors="ignore").sort_values(key=abs,ascending=False).head(5)
            if not _corr_op.empty:
                st.markdown("**영업이익과 상관도 높은 지표:**")
                for _fn,_rv in _corr_op.items():
                    _dir="양의" if _rv>0 else "음의"
                    st.markdown(f"- **{_fn}**: {_dir} 상관 (r={_rv:.2f})")
        else:
            st.info("Correlation 계산에 최소 3개 캠페인이 필요합니다.")

    with _t6e:
        sec("📐 통계 Lab (OLS 회귀 / VIF / Spend 탄력성 / 요일 효과)")

        # 일별 집계 데이터 준비
        _daily_ml = df.groupby("date").agg({
            c: "sum" for c in ["spend", "sales", "clicks", "impressions", "orders", "units"] if c in df.columns
        }).reset_index()
        _daily_ml["roas"] = _daily_ml["sales"] / _daily_ml["spend"].replace(0, np.nan)
        _daily_ml["acos_pct"] = _daily_ml["spend"] / _daily_ml["sales"].replace(0, np.nan) * 100
        _daily_ml["ctr_pct"] = _daily_ml["clicks"] / _daily_ml["impressions"].replace(0, np.nan) * 100
        _daily_ml["cvr_ord_pct"] = _daily_ml["orders"] / _daily_ml["clicks"].replace(0, np.nan) * 100
        _daily_ml["cpc"] = _daily_ml["spend"] / _daily_ml["clicks"].replace(0, np.nan)
        _daily_ml["aov"] = _daily_ml["sales"] / _daily_ml["orders"].replace(0, np.nan)
        _daily_ml["dow"] = _daily_ml["date"].dt.dayofweek

        st.caption("회귀분석(OLS) · VIF 다중공선성 · Spend 탄력성 · Profit Drivers · 요일 효과")

        if len(_daily_ml) < 8:
            st.warning(f"⚠ 일별 데이터 {len(_daily_ml)}행 — 통계 분석에 최소 8일 이상 필요.")
        else:
            _ml_tabs = st.tabs(["📈 Spend→Sales", "📊 CVR/CPC→ROAS", "🎯 Bid→ROAS", "💰 Profit Drivers", "📅 요일 효과"])

            # ── ML helper: OLS regression ──
            def _ols_regression(data, x_cols, y_col):
                clean = data[x_cols + [y_col]].replace([np.inf, -np.inf], np.nan).dropna()
                if len(clean) < 5: return {"error": f"데이터 부족 ({len(clean)}행)"}
                X = clean[x_cols].values
                y = clean[y_col].values
                X_int = np.column_stack([np.ones(len(X)), X])
                try:
                    beta = np.linalg.lstsq(X_int, y, rcond=None)[0]
                    y_pred = X_int @ beta
                    ss_res = np.sum((y - y_pred) ** 2)
                    ss_tot = np.sum((y - np.mean(y)) ** 2)
                    r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0
                    coeffs = dict(zip(["intercept"] + x_cols, beta))
                    return {"r2": r2, "coefficients": coeffs, "n": len(clean),
                            "y_pred": y_pred, "X": clean[x_cols], "y": y}
                except Exception as e:
                    return {"error": str(e)}

            def _compute_vif(data, cols):
                clean = data[cols].replace([np.inf, -np.inf], np.nan).dropna()
                if len(clean) < 5: return pd.DataFrame()
                vifs = []
                for i, col in enumerate(cols):
                    others = [c for c in cols if c != col]
                    X = clean[others].values; y = clean[col].values
                    X_int = np.column_stack([np.ones(len(X)), X])
                    try:
                        beta = np.linalg.lstsq(X_int, y, rcond=None)[0]
                        y_pred = X_int @ beta
                        ss_res = np.sum((y - y_pred) ** 2)
                        ss_tot = np.sum((y - np.mean(y)) ** 2)
                        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0
                        vif = 1.0 / (1.0 - r2) if r2 < 1 else 999999.0
                    except:
                        vif = float("nan")
                    vifs.append({"Feature": col, "VIF": round(vif, 2),
                                 "판정": "⚠ 다중공선성" if vif > 5 else "✅ 양호"})
                return pd.DataFrame(vifs)

            def _ml_block(data, x_cols, y_col, scatter_x, scatter_y, title, key_prefix):
                avail_x = [c for c in x_cols if c in data.columns]
                clean = data[avail_x + [y_col]].replace([np.inf, -np.inf], np.nan).dropna()
                if len(clean) < 5:
                    st.info(f"데이터 {len(clean)}행 — 최소 5행 필요."); return

                # Scatter + Trendline
                if scatter_x in clean.columns and scatter_y in clean.columns:
                    _fig_sc = go.Figure()
                    _fig_sc.add_trace(go.Scatter(
                        x=clean[scatter_x], y=clean[scatter_y], mode="markers",
                        marker=dict(size=8, color=C["spend"], opacity=0.6), name="Data Points"))
                    reg_simple = _ols_regression(clean, [scatter_x], scatter_y)
                    if "error" not in reg_simple:
                        _x_range = np.linspace(clean[scatter_x].min(), clean[scatter_x].max(), 50)
                        _y_hat = reg_simple["coefficients"]["intercept"] + \
                                 reg_simple["coefficients"][scatter_x] * _x_range
                        _fig_sc.add_trace(go.Scatter(
                            x=_x_range, y=_y_hat, mode="lines",
                            line=dict(color="#EF4444", width=2, dash="dash"),
                            name=f"OLS (R²={reg_simple['r2']:.3f})"))
                    _fig_sc.update_layout(**{**BASE_LAYOUT, "height": 350, "title": title,
                                             "xaxis_title": scatter_x, "yaxis_title": scatter_y})
                    st.plotly_chart(_fig_sc, use_container_width=True)

                # VIF
                if len(avail_x) >= 2:
                    st.markdown("**🔍 VIF — 다중공선성 진단**")
                    vif_df = _compute_vif(clean, avail_x)
                    if not vif_df.empty:
                        st.dataframe(vif_df, use_container_width=True, hide_index=True)
                        high_vif = vif_df[vif_df["VIF"] > 5]
                        if not high_vif.empty:
                            st.caption("⚠️ VIF > 5: Feature 간 상관이 높아 Coefficient 불안정 가능")
                        else:
                            st.caption("✅ VIF < 5: 다중공선성 없음")

                # OLS Regression
                st.markdown(f"**📉 OLS Regression: {avail_x} → {y_col}**")
                reg = _ols_regression(clean, avail_x, y_col)
                if "error" in reg:
                    st.error(reg["error"])
                else:
                    st.metric("R² Score", f"{reg['r2']:.4f}",
                              delta="Good fit" if reg['r2'] > 0.5 else "Weak fit",
                              delta_color="normal" if reg['r2'] > 0.5 else "inverse")
                    coef_df = pd.DataFrame([{"Feature": k, "Coefficient": f"{v:.6f}"}
                                             for k, v in reg["coefficients"].items()])
                    st.dataframe(coef_df, use_container_width=True, hide_index=True)

            # ══════ ML-1: Spend → Sales + Elasticity ══════
            with _ml_tabs[0]:
                st.caption("Spend(광고비)가 Sales(매출)에 미치는 영향")
                _ml_block(_daily_ml, ["spend", "clicks"], "sales",
                          "spend", "sales", "Spend → Sales", "ml0")

                st.divider()
                sec("📊 Spend Elasticity (탄력성, log-log)")
                _camp_daily = agg_kpis(df, ["date", "campaign"])
                _camp_daily = _camp_daily[(_camp_daily["spend"] > 0) & (_camp_daily["sales"] > 0)]
                if len(_camp_daily) >= 15:
                    _camp_daily["log_spend"] = np.log(_camp_daily["spend"])
                    _camp_daily["log_sales"] = np.log(_camp_daily["sales"])
                    _elas_results = []
                    for camp in _camp_daily["campaign"].unique():
                        _cd = _camp_daily[_camp_daily["campaign"] == camp]
                        if len(_cd) >= 10:
                            reg_e = _ols_regression(_cd, ["log_spend"], "log_sales")
                            if "error" not in reg_e:
                                elas = reg_e["coefficients"].get("log_spend", 0)
                                _elas_results.append({
                                    "캠페인": camp, "Elasticity": round(elas, 3),
                                    "R²": round(reg_e["r2"], 3), "일수": len(_cd),
                                    "해석": "수확체증" if elas > 1 else "수확체감" if elas > 0 else "역효과"
                                })
                    if _elas_results:
                        st.dataframe(pd.DataFrame(_elas_results), use_container_width=True, hide_index=True)
                        st.caption("Elasticity > 1: Spend↑ → Sales가 더 크게↑. < 1: Diminishing Returns.")
                    else:
                        st.info("캠페인당 최소 10일 이상의 일별 데이터 필요.")
                else:
                    st.info("Elasticity 분석에 충분한 데이터가 없습니다.")

            # ══════ ML-2: CVR/CPC/AOV → ROAS ══════
            with _ml_tabs[1]:
                st.caption("ROAS = (CVR × AOV) / CPC — 어떤 Feature가 ROAS에 가장 큰 영향?")
                _ml_block(_daily_ml, ["cvr_ord_pct", "cpc", "aov"], "roas",
                          "cvr_ord_pct", "roas", "CVR/CPC/AOV → ROAS", "ml1")

            # ══════ ML-3: Bid → ROAS ══════
            with _ml_tabs[2]:
                st.caption("Bid(입찰가) vs ROAS 관계 분석 — CPC를 Bid 근사치로 사용")
                # CPC를 Bid proxy로 사용 (실제 Bid 데이터가 없는 경우)
                _bid_data = _ai_camp[["campaign", "cpc", "roas", "spend", "sales"]].dropna()
                _bid_data = _bid_data[(_bid_data["cpc"] > 0) & (_bid_data["roas"].between(0, 50))]
                if len(_bid_data) >= 5:
                    _fig_bid_ml = go.Figure()
                    _fig_bid_ml.add_trace(go.Scatter(
                        x=_bid_data["cpc"], y=_bid_data["roas"], mode="markers",
                        marker=dict(size=10, color=C["spend"], opacity=0.6),
                        text=_bid_data["campaign"].str[:20], name="캠페인"))
                    reg_bid_ml = _ols_regression(_bid_data, ["cpc"], "roas")
                    if "error" not in reg_bid_ml:
                        _xr = np.linspace(_bid_data["cpc"].min(), _bid_data["cpc"].max(), 30)
                        _yr = reg_bid_ml["coefficients"]["intercept"] + reg_bid_ml["coefficients"]["cpc"] * _xr
                        _fig_bid_ml.add_trace(go.Scatter(
                            x=_xr, y=_yr, mode="lines",
                            line=dict(color="#EF4444", width=2, dash="dash"),
                            name=f"OLS (R²={reg_bid_ml['r2']:.3f})"))
                    _fig_bid_ml.update_layout(**{**BASE_LAYOUT, "height": 350,
                                                  "title": "CPC(Bid proxy) vs ROAS",
                                                  "xaxis_title": "CPC ($)", "yaxis_title": "ROAS"})
                    st.plotly_chart(_fig_bid_ml, use_container_width=True)
                    if "error" not in reg_bid_ml:
                        bid_coef = reg_bid_ml["coefficients"].get("cpc", 0)
                        st.info(f"💡 CPC Coefficient: {bid_coef:.4f} — "
                                f"{'CPC↑ → ROAS↑ (높은 입찰이 유리)' if bid_coef > 0 else 'CPC↑ → ROAS↓ (과다입찰 주의, 보수적 전략 권장)'}")
                        st.metric("R² Score", f"{reg_bid_ml['r2']:.4f}")
                else:
                    st.info(f"Bid 분석에 캠페인 5개 이상 필요합니다. (현재: {len(_bid_data)}개)")

            # ══════ ML-4: Profit Drivers ══════
            with _ml_tabs[3]:
                st.caption("어떤 Feature가 영업이익에 가장 큰 영향?")
                _ml_block(_ai_camp, ["spend", "cpc", "cvr_pct", "aov", "clicks"], "profit_proxy",
                          "spend", "profit_proxy", "Spend/CPC/CVR/AOV → Profit", "ml3")

            # ══════ ML-5: Day-of-Week Effect ══════
            with _ml_tabs[4]:
                sec("📅 요일 효과 Regression")
                st.caption("요일이 Sales에 통계적으로 유의한 영향을 주는지?")
                if len(_daily_ml) >= 8:
                    _dow_dummies = pd.get_dummies(_daily_ml["dow"], prefix="day").astype(float)
                    _daily_dow = pd.concat([_daily_ml[["sales", "spend"]].astype(float), _dow_dummies], axis=1)
                    _dow_feats = ["spend"] + [c for c in _dow_dummies.columns if c != "day_0"]
                    reg_dow = _ols_regression(_daily_dow, _dow_feats, "sales")
                    if "error" not in reg_dow:
                        day_names = {1: "화(Tue)", 2: "수(Wed)", 3: "목(Thu)",
                                     4: "금(Fri)", 5: "토(Sat)", 6: "일(Sun)"}
                        dow_coefs = []
                        for feat, coef in reg_dow["coefficients"].items():
                            if feat.startswith("day_"):
                                day_num = int(feat.split("_")[1])
                                dow_coefs.append({
                                    "요일": day_names.get(day_num, feat),
                                    "Sales 효과 ($)": fu(coef),
                                    "해석": "기준(Mon) 대비 " + ("↑ 매출 증가" if coef > 0 else "↓ 매출 감소")
                                })
                        if dow_coefs:
                            st.dataframe(pd.DataFrame(dow_coefs), use_container_width=True, hide_index=True)
                            st.caption(f"R² = {reg_dow['r2']:.3f}. 월요일(Mon) 대비 각 요일의 Sales 차이.")
                    else:
                        st.error(reg_dow.get("error", "Regression 실패"))
                # 요일별 차트 (기존)
                _wmap={"Monday":"월","Tuesday":"화","Wednesday":"수","Thursday":"목",
                       "Friday":"금","Saturday":"토","Sunday":"일"}
                _dfw6=df.copy()
                _dfw6["_wday"]=_dfw6["date"].dt.day_name().map(_wmap)
                _wg6=agg_kpis(_dfw6,["_wday"]).rename(columns={"_wday":"요일"})
                _wg6["요일"]=pd.Categorical(_wg6["요일"],categories=["월","화","수","목","금","토","일"],ordered=True)
                _wg6=_wg6.sort_values("요일")
                _wc1,_wc2=st.columns(2)
                with _wc1:
                    fig_wd6=go.Figure()
                    fig_wd6.add_trace(go.Bar(x=_wg6["요일"],y=_wg6["spend"],name="Spend",marker_color=C["spend"]))
                    fig_wd6.add_trace(go.Bar(x=_wg6["요일"],y=_wg6["sales"],name="Sales",marker_color=C["sales"]))
                    fig_wd6.update_layout(**{**BASE_LAYOUT,"barmode":"group","height":270,"title":"요일별 Spend/Sales"})
                    st.plotly_chart(fig_wd6,use_container_width=True)
                with _wc2:
                    fig_wd62=go.Figure()
                    fig_wd62.add_trace(go.Bar(x=_wg6["요일"],y=_wg6["acos_pct"],name="ACoS(%)",
                                               marker_color=C["acos"],text=_wg6["acos_pct"].map(fp),textposition="outside"))
                    fig_wd62.add_hline(y=TARGET_ACOS,line_dash="dash",line_color="#EF4444")
                    fig_wd62.update_layout(**{**BASE_LAYOUT,"height":270,"title":"요일별 ACoS"})
                    st.plotly_chart(fig_wd62,use_container_width=True)

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # 운영 관리 블록
    st.divider()
    st.header("🏛 운영 관리")

    # ── 데이터 소스 맵 ──
    sec("🗺️ 데이터 소스 맵 — 어떤 탭에서 어떤 데이터를 쓰는가?")
    with st.expander("📖 전체 탭 × 데이터 매핑", expanded=False):
        st.markdown(f"""
| 탭 | 주요 데이터 | 비고 |
|---|---|---|
| 📊 운영 현황 | fact_ads (SSOT) | KPI·차트·검색어·드릴다운 |
| 💰 CEO 전략 | fact_ads + 원가 | Verdict·BEP·시뮬레이션 |
| 📈 Profit Sim | fact_ads + 원가 | Incrementality·재배분·로드맵 |
| 🏗 제품 구조 | fact_ads + COGS | SKU P&L·Tier·BE Gap |
| ⚔ 판매팀 액션 | fact_ads + Verdict | 우선순위별 실행 계획 |
| 🤖 AI & ML Lab | fact_ads (캠페인/일별 집계) | RF·KMeans·DTree·Regression |

**데이터 현황:**
- fact_ads: **{len(df):,}행** · {df['campaign'].nunique()}개 캠페인 · {D0.date()}~{D1.date()}
- 원가 마스터: {'✅ 연결' if HAS_COGS else '❌ 미연결'}
- Profit 공식: `operating_profit = sales − units × unit_cogs_adj − spend`
""")

    st.divider()

    # ── 액션 히스토리 ──
    sec("📜 액션 히스토리")
    if "action_log" not in st.session_state:
        st.session_state["action_log"] = []
    _log = st.session_state["action_log"]

    _al1, _al2 = st.columns([3, 1])
    with _al1:
        _new_action = st.text_input("새 액션 기록", placeholder="예: SP-Auto 캠페인 Bid 20% 인하", key="al_input")
    with _al2:
        _action_cat = st.selectbox("카테고리", ["Bid 조정", "Pause/Resume", "Negative 등록",
                                               "예산 변경", "타겟 변경", "기타"], key="al_cat")
    if st.button("➕ 기록 추가", key="al_add"):
        if _new_action.strip():
            _log.append({
                "일시": datetime.now().strftime("%Y-%m-%d %H:%M"),
                "카테고리": _action_cat,
                "액션": _new_action,
                "상태": "⏳ 진행중",
            })
            st.session_state["action_log"] = _log
            st.success(f"✅ '{_new_action}' 기록 완료")

    if _log:
        st.dataframe(pd.DataFrame(_log), use_container_width=True, hide_index=True)
        if st.button("🗑 전체 초기화", key="al_clear"):
            st.session_state["action_log"] = []
            st.rerun()
    else:
        st.info("아직 기록된 액션이 없습니다. 위에서 액션을 추가하세요.")

    st.divider()

    # ── Playbook 자동 생성 ──
    sec("📦 Playbook 자동 생성")
    st.caption("현재 데이터 기반으로 실행 가이드를 자동 생성합니다.")

    if st.button("🚀 Generate Playbook", use_container_width=True, key="gen_playbook"):
        with st.spinner("Playbook 생성 중..."):
            _cv_pb = agg_kpis(df, ["campaign"])
            _cv_pb["verdict"] = _cv_pb.apply(lambda r: _apply_verdict(r, _BE_ROAS_TOT, IS_GROWTH_MODE, TARGET_ACOS), axis=1)
            _cuts_pb = _cv_pb[_cv_pb["verdict"].str.contains("CUT")]
            _fixes_pb = _cv_pb[_cv_pb["verdict"].str.contains("FIX")]
            _scales_pb = _cv_pb[_cv_pb["verdict"].str.contains("SCALE")]
            _neg_pb = agg_kpis(df, ["search_term", "campaign"])
            _neg_pb = _neg_pb[(_neg_pb["clicks"] >= 10) & (_neg_pb["orders"] == 0)]

            _pb_text = f"""# 📦 PPC Playbook — Auto-Generated
## 기간: {D0.date()} ~ {D1.date()}

---

## 📊 현황 요약
- **Spend:** {fu(_SPEND)} | **Sales:** {fu(_SALES)} | **ROAS:** {fx(_ROAS)}
- **BE ROAS:** {fx(_BE_ROAS_TOT) if _BE_ROAS_TOT < 900 else '∞'} | **영업이익:** {fu(_PROFIT)}
- **캠페인:** {df['campaign'].nunique()}개 | **검색어:** {df['search_term'].nunique():,}개

---

## 🚨 Week 1: 즉시 실행
### 1. CUT 캠페인 Pause ({len(_cuts_pb)}개)
"""
            for _, r in _cuts_pb.iterrows():
                _pb_text += f"- [ ] {r['campaign']} — Spend {fu(r['spend'])}, ROAS {fx(r['roas'])}\n"
            _pb_text += f"""
### 2. Negative 키워드 등록 ({len(_neg_pb)}개)
- 낭비 합계: {fu(float(_neg_pb['spend'].sum()))}
- [ ] 검색어 리스트 다운 → Negative Exact 등록

---

## 🔧 Week 2-3: 최적화
### 3. FIX 캠페인 Bid/타겟 조정 ({len(_fixes_pb)}개)
"""
            for _, r in _fixes_pb.iterrows():
                _pb_text += f"- [ ] {r['campaign']} — Spend {fu(r['spend'])}, ROAS {fx(r['roas'])}\n"
            _pb_text += f"""
---

## 📈 Month 2+: 확장
### 4. SCALE 캠페인 예산 확대 ({len(_scales_pb)}개)
"""
            for _, r in _scales_pb.iterrows():
                _pb_text += f"- [ ] {r['campaign']} — Spend {fu(r['spend'])}, ROAS {fx(r['roas'])}\n"
            _pb_text += f"""
---

## 📋 원가 모델
- Profit 공식: `operating_profit = sales − units × total_unit_cost − spend`
- 제품원가 조정: {_cost_mult:+.0%}
- Amazon 수수료: {_amazon_fee_pct}% | FBA 배송비: ${_fba_ship:.1f}/unit
- 반품률: {_return_pct}%
- 1P 비용: COOP {_coop_pct}% · DA {_da_pct}% · FA {_fa_pct}%

_Generated by Neo PPC v3.2_
"""
            st.text_area("Playbook Preview", _pb_text, height=400, key="pb_preview")
            st.download_button("📥 Playbook 다운로드 (.md)",
                               _pb_text.encode("utf-8"), "ppc_playbook.md",
                               mime="text/markdown", key="dl_playbook")
            st.success("✅ Playbook 생성 완료!")

    # ── Forecast (WIP) 서브탭 ──
    with _t6f:
        sec("🔬 Forecast & 예측")
        with st.expander("📖 Forecast는 어떻게 보나요?", expanded=False):
            st.markdown("""
**현재 Phase 1:** 1개월 데이터로 가능한 분석
- 일별 추세 기반 **다음 달 예측** (선형 외삽)
- 주간 패턴 기반 **요일별 예상 성과**
- 데이터 충분성 체크

**향후 Phase 2~4:** 데이터가 3개월+ 쌓이면 더 정교한 예측 활성화
""")
        st.divider()

        sec("📋 데이터 준비 상태 체크")
        _ml_c1,_ml_c2,_ml_c3,_ml_c4=st.columns(4)
        _data_days = (D1-D0).days+1
        _ml_c1.metric("데이터 기간 (일)",str(_data_days),"30일 이상 필요" if _data_days<30 else "✅")
        _ml_c2.metric("캠페인 수",str(df["campaign"].nunique()),"5개 이상 필요" if df["campaign"].nunique()<5 else "✅")
        _ml_c3.metric("ASIN 원가 매칭률",fp(_cov["match_pct"]),"80% 이상 권장" if _cov["match_pct"]<80 else "✅")
        _ml_c4.metric("총 행 수",fi(len(df)),"1,000행 이상 권장" if len(df)<1000 else "✅")
        st.divider()

        # ── 일별 추세 기반 다음 달 예측 ──
        sec("📈 다음 달 예측 (선형 추세 외삽)")
        _fc_daily = df.groupby("date").agg(
            spend=("spend","sum"), sales=("sales","sum"),
            orders=("orders","sum"), clicks=("clicks","sum")
        ).reset_index().sort_values("date")

        if len(_fc_daily) >= 14:
            _fc_daily["day_num"] = range(len(_fc_daily))
            # 선형 회귀로 추세 추정
            from numpy.polynomial.polynomial import polyfit
            _fc_x = _fc_daily["day_num"].values.astype(float)

            _fc_results = {}
            for col in ["spend", "sales", "orders"]:
                _fc_y = _fc_daily[col].values.astype(float)
                try:
                    _coefs = polyfit(_fc_x, _fc_y, 1)
                    _daily_avg = _fc_y.mean()
                    _trend_per_day = _coefs[1]
                    _next_month_avg = _daily_avg + _trend_per_day * 30
                    _fc_results[col] = {
                        "current_daily": _daily_avg,
                        "trend": _trend_per_day,
                        "next_month_daily": max(0, _next_month_avg),
                        "next_month_total": max(0, _next_month_avg * 30)
                    }
                except:
                    pass

            if _fc_results:
                _fc_r1, _fc_r2, _fc_r3 = st.columns(3)
                if "sales" in _fc_results:
                    _fc_r1.metric(
                        "예상 월 매출", fu(_fc_results["sales"]["next_month_total"]),
                        f"일 {fu(_fc_results['sales']['next_month_daily'])} (추세 {'+' if _fc_results['sales']['trend'] > 0 else ''}{fu(_fc_results['sales']['trend'])}/일)")
                if "spend" in _fc_results:
                    _fc_r2.metric(
                        "예상 월 Spend", fu(_fc_results["spend"]["next_month_total"]),
                        f"일 {fu(_fc_results['spend']['next_month_daily'])}")
                if "orders" in _fc_results:
                    _fc_r3.metric(
                        "예상 월 주문", f"{_fc_results['orders']['next_month_total']:.0f}건",
                        f"일 {_fc_results['orders']['next_month_daily']:.1f}건")

                # 예상 ROAS, ACoS
                if "sales" in _fc_results and "spend" in _fc_results:
                    _fc_next_roas = _fc_results["sales"]["next_month_total"] / _fc_results["spend"]["next_month_total"] if _fc_results["spend"]["next_month_total"] > 0 else 0
                    _fc_next_acos = _fc_results["spend"]["next_month_total"] / _fc_results["sales"]["next_month_total"] * 100 if _fc_results["sales"]["next_month_total"] > 0 else 999
                    _fc_r4, _fc_r5, _fc_r6 = st.columns(3)
                    _fc_r4.metric("예상 ROAS", f"{_fc_next_roas:.2f}x", f"현재 {_ROAS:.2f}x")
                    _fc_r5.metric("예상 ACoS", f"{_fc_next_acos:.1f}%", f"현재 {_ACOS:.1f}%")
                    if "orders" in _fc_results and _fc_results["orders"]["next_month_total"] > 0:
                        _fc_next_profit = _fc_results["sales"]["next_month_total"] - _fc_results["orders"]["next_month_total"] * (_UNITS/max(_ORDERS,1)) * _AVG_COGS_PU - _fc_results["spend"]["next_month_total"]
                        _fc_r6.metric("예상 영업이익", fu(_fc_next_profit),
                                      "흑자 예상" if _fc_next_profit > 0 else "적자 예상",
                                      delta_color="normal" if _fc_next_profit > 0 else "inverse")

                # 추세 차트
                _fig_fc = go.Figure()
                _fig_fc.add_trace(go.Scatter(x=_fc_daily["date"], y=_fc_daily["sales"],
                                              mode="lines+markers", name="실제 Sales", line=dict(color=C["sales"])))
                # 추세선 연장
                import datetime
                _future_dates = [_fc_daily["date"].max() + datetime.timedelta(days=i) for i in range(1, 31)]
                _future_x = [len(_fc_daily) + i for i in range(30)]
                if "sales" in _fc_results:
                    _coefs_s = polyfit(_fc_x, _fc_daily["sales"].values.astype(float), 1)
                    _trend_y = [_coefs_s[0] + _coefs_s[1] * x for x in _future_x]
                    _fig_fc.add_trace(go.Scatter(x=_future_dates, y=_trend_y,
                                                  mode="lines", name="추세 예측",
                                                  line=dict(color="#EF4444", width=2, dash="dash")))
                _fig_fc.update_layout(**{**BASE_LAYOUT, "height": 320, "title": "Sales 추세 + 다음 달 예측"})
                st.plotly_chart(_fig_fc, use_container_width=True)

                st.caption("⚠️ 선형 추세 외삽은 단순 참고용. 시즌, 경쟁, 재고 변동은 미반영.")
        else:
            st.info(f"추세 예측에 최소 14일 데이터 필요 (현재: {len(_fc_daily)}일)")
        st.divider()

        # ── Phase 로드맵 ──
        with st.expander("📌 Phase 로드맵"):
            st.markdown("""
| Phase | 기능 | 필요 데이터 | 상태 |
|-------|------|------------|------|
| **1 (현재)** | SSOT 분석 · BE 진단 · 시뮬레이션 · 선형 예측 | 1개월 CSV + 원가 | ✅ 활성 |
| 2 | Spend 탄력성 회귀 · 입찰 최적화 | 3개월 이상 누적 | 🔒 데이터 부족 |
| 3 | Time-Series Forecast (Prophet/LSTM) | 6개월 이상 + 외부 변수 | 🔒 데이터 부족 |
| 4 | RL 기반 Budget 자동 배분 | API 연동 + 실시간 데이터 | 🔒 미구현 |
""")


# ╔══════════════════════════════════════════════════════════════════╗
# ║  TAB 7 — 설정 (원가 신뢰도 & 데이터 품질)                       ║
# ╚══════════════════════════════════════════════════════════════════╝
with TAB7:
    st.header("🔧 원가 신뢰도 & 데이터 품질")
    st.caption("ASIN 기반 원가 매칭률 · 미매칭 ASIN · 원가 마스터 · SKU P&L · BE 시나리오")

    # ── § 0 원가 매핑 현황 ────────────────────────────────────────
    sec("① ASIN 원가 매핑 현황")
    _t8_cov=cogs_coverage(fact_merged_base)
    _t8c1,_t8c2,_t8c3=st.columns(3)
    _t8c1.metric("ASIN 매칭률",fp(_t8_cov["match_pct"]),
                 f"{_t8_cov['matched_asin']}/{_t8_cov['total_asin']} ASIN")
    _t8c2.metric("Spend 커버리지",fp(_t8_cov["spend_matched_pct"]),
                 "실제 원가 기반 Spend 비율")
    _t8c3.metric("전체 데이터 행",fi(len(fact_merged_base)),
                 f"원가 매칭 {fi(int(fact_merged_base['unit_cogs'].notna().sum()))}행")

    if _t8_cov["match_pct"]>=90:
        st.success(f"✅ 원가 매칭 우수 ({_t8_cov['match_pct']:.0f}%)")
    elif _t8_cov["match_pct"]>=70:
        st.warning(f"⚠️ 원가 매칭 보통 ({_t8_cov['match_pct']:.0f}%) — 미매칭 ASIN 원가 추가 권장")
    else:
        st.error(f"❌ 원가 매칭 부족 ({_t8_cov['match_pct']:.0f}%) — 영업이익/BE 정확도 저하 가능")
    st.divider()

    # ── § 1 미매칭 ASIN 리스트 ────────────────────────────────────
    if _t8_cov["match_pct"]<100:
        sec("② 미매칭 ASIN (원가 추가 필요)")
        _unmatched=(
            fact_merged_base[fact_merged_base["unit_cogs"].isna()&fact_merged_base["asin"].notna()]
            .groupby("asin").agg({"spend":"sum","sales":"sum","orders":"sum"})
            .sort_values("spend",ascending=False).reset_index()
        )
        _unmatched["Proxy 원가"]=(_unmatched["sales"]/_unmatched["orders"].replace(0,np.nan)*0.70).map(fu)
        _unmatched["spend"]=_unmatched["spend"].map(fu)
        _unmatched["sales"]=_unmatched["sales"].map(fu)
        _unmatched["orders"]=_unmatched["orders"].map(fi)
        st.warning(f"⚠️ 미매칭 ASIN {len(_unmatched)}개 — Spend 높은 순 정렬 (Proxy 원가 = avg×70% 임시 적용)")
        st.dataframe(_unmatched.rename(columns={"asin":"ASIN","spend":"Spend($)","sales":"Sales($)","orders":"주문"}),
                     use_container_width=True,hide_index=True)
        st.download_button("📥 미매칭 ASIN CSV",_unmatched.to_csv(index=False).encode("utf-8-sig"),
                           "unmatched_asin.csv",key="dl_unmatched8")
        st.divider()

    # ── § 2 원가 마스터 미리보기 ──────────────────────────────────
    if HAS_COGS:
        sec("③ 원가 마스터 테이블")
        _show_n=st.number_input("미리보기 행 수",5,500,50,step=10,key="t8_n")
        st.dataframe(cogs_df.head(int(_show_n)).rename(columns={"asin":"ASIN","sku_cogs":"SKU","unit_cogs":"단위원가($)"}),
                     use_container_width=True,hide_index=True)
        st.caption(f"총 {len(cogs_df)} ASIN | 원가 범위: ${cogs_df['unit_cogs'].min():.2f} ~ ${cogs_df['unit_cogs'].max():.2f} | 평균: ${cogs_df['unit_cogs'].mean():.2f}")
        st.divider()

        # ── § 3 SKU별 BE ACoS Gap 분석 ───────────────────────────
        sec("④ SKU별 BE ACoS Gap 분석")
        _t8_asin=fact_merged_base[fact_merged_base["unit_cogs"].notna()].copy()
        if not _t8_asin.empty:
            _t8_p=transform_add_profit(_t8_asin,_cost_mult,_fee_frac,_fba_ship,_ret_frac,_coop_frac,_da_frac,_fa_frac,_wholesale_ratio,_deal_frac)
            _t8g=agg_kpis(_t8_p,["asin","sku"])
            _t8g["display_id"]=_t8g["sku"].fillna(_t8g["asin"])
            _t8g["be_acos_str"]=_t8g["be_acos"].map(lambda v:fp(v) if pd.notna(v) else "구조적 적자")
            _t8g["acos_str"]=_t8g["acos_pct"].map(fp)
            _t8g["gap"]=_t8g["acos_pct"]-_t8g["be_acos"].fillna(0)
            _t8g_d=_t8g.sort_values("gap",ascending=False)
            _gap_cols=["display_id","spend","sales","orders","acos_pct","be_acos","gap","operating_profit","structural_loss"]
            _gap_cols=[c for c in _gap_cols if c in _t8g_d.columns]
            _t8g_fmt=_t8g_d[_gap_cols].copy()
            for _mc in ["spend","sales","operating_profit"]:
                if _mc in _t8g_fmt.columns: _t8g_fmt[_mc]=_t8g_fmt[_mc].map(fu)
            for _pc in ["acos_pct","be_acos","gap"]:
                if _pc in _t8g_fmt.columns: _t8g_fmt[_pc]=_t8g_fmt[_pc].map(lambda v:fp(v) if pd.notna(v) else "—")
            _t8g_fmt["orders"]=_t8g_fmt["orders"].map(fi)
            def _gap_hl(row):
                if row.get("structural_loss",False): return ["background-color:#FEE2E2"]*len(row)
                return [""]*len(row)
            st.dataframe(_t8g_fmt.rename(columns={"display_id":"SKU/ASIN","spend":"Spend($)","sales":"Sales($)",
                "orders":"주문","acos_pct":"ACoS(%)","be_acos":"BE ACoS(%)","gap":"Gap(ACoS-BE)",
                "operating_profit":"영업이익($)","structural_loss":"구조적 적자"}).style.apply(_gap_hl,axis=1),
                use_container_width=True,hide_index=True,height=360)
            # Gap bar chart
            _t8g_top=_t8g_d.head(15)
            fig_gap=go.Figure()
            fig_gap.add_trace(go.Bar(x=_t8g_top["display_id"],y=_t8g_top["acos_pct"],
                                     name="실제 ACoS(%)",marker_color=C["acos"]))
            fig_gap.add_trace(go.Scatter(x=_t8g_top["display_id"],
                                          y=_t8g_top["be_acos"].clip(lower=0),
                                          name="BE ACoS(%)",mode="markers",
                                          marker=dict(size=10,color="#EF4444",symbol="line-ew-open",line=dict(width=3))))
            fig_gap.update_layout(**{**BASE_LAYOUT,"height":340,"title":"SKU별 실제 ACoS vs BE ACoS",
                                     "xaxis_tickangle":-40,"barmode":"group"})
            st.plotly_chart(fig_gap,use_container_width=True)
            st.divider()

        # ── § 4 BE ROAS 민감도 Heatmap ────────────────────────────
        sec("⑤ BE ROAS 민감도 — Cost Multiplier × SKU")
        _t8g2=agg_kpis(_t8_p,["asin","sku"])
        _t8g2["display_id"]=_t8g2["sku"].fillna(_t8g2["asin"])
        _top_skus=_t8g2.sort_values("spend",ascending=False).head(10)["display_id"].tolist()
        _mult_range=np.arange(-0.10,0.11,0.05)
        _heat_rows=[]; _heat_cols=[f"{m:+.0%}" for m in _mult_range]
        for _asin in _top_skus:
            _row_d=_t8_asin[_t8_asin["asin"]==_t8g2[_t8g2["display_id"]==_asin]["asin"].values[0]] if _t8g2[_t8g2["display_id"]==_asin]["asin"].notna().any() else pd.DataFrame()
            if _row_d.empty: continue
            _row_vals=[]
            for _m in _mult_range:
                _tp=transform_add_profit(_row_d,_m,_fee_frac,_fba_ship,_ret_frac,_coop_frac,_da_frac,_fa_frac,_wholesale_ratio,_deal_frac)
                _be=float(_tp["be_spend"].sum())
                _sa=float(_tp["sales"].sum())
                _be_roas=_sa/_be if _be>0 else 999.
                _row_vals.append(round(_be_roas,2))
            _heat_rows.append(_row_vals)
        if _heat_rows:
            _top_skus_used=_top_skus[:len(_heat_rows)]
            fig_hm8=go.Figure(go.Heatmap(
                z=_heat_rows,x=_heat_cols,y=_top_skus_used,colorscale="RdYlGn",
                text=[[fx(v) if v<900 else "∞" for v in row] for row in _heat_rows],
                texttemplate="%{text}",textfont=dict(size=9),
                colorbar=dict(title="BE ROAS")))
            fig_hm8.update_layout(**{**BASE_LAYOUT,"height":360,"title":"BE ROAS 민감도 (Cost Multiplier × SKU)",
                                     "xaxis_title":"Cost Multiplier","margin":dict(l=140,r=80,t=50,b=4)})
            st.plotly_chart(fig_hm8,use_container_width=True)
    else:
        st.info("ℹ️ 원가 마스터를 업로드하면 원가 기반 분석이 활성화됩니다.")
    st.divider()

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # §5-b  P&L Waterfall 차트 (all-in 원가 기반)
    if HAS_COGS:
        sec("⑥ P&L Waterfall (원가 기반)")
        _wf_sales = _SALES
        _wf_cogs  = -_TOTAL_COGS_ADJ                # units × unit_cogs_adj (all-in)
        _wf_spend = -_SPEND
        _wf_profit = _PROFIT
        _wf_labels  = ["Sales", "COGS (all-in)", "Ad Spend", "영업이익"]
        _wf_values  = [_wf_sales, _wf_cogs, _wf_spend, _wf_profit]
        _wf_measure = ["absolute", "relative", "relative", "total"]
        fig_wf = go.Figure(go.Waterfall(
            x=_wf_labels, y=_wf_values, measure=_wf_measure,
            text=[fu(v) for v in _wf_values], textposition="outside",
            connector=dict(line=dict(color="#94A3B8", width=1)),
            increasing=dict(marker_color=C["profit"]),
            decreasing=dict(marker_color=C["loss"]),
            totals=dict(marker_color=C["roas"]),
        ))
        fig_wf.update_layout(**{**BASE_LAYOUT, "height": 380,
                                "title": "P&L Waterfall (all-in 원가 기반)"})
        st.plotly_chart(fig_wf, use_container_width=True)
        st.caption("unit_cogs는 all-in cost (수수료/배송/반품 포함). 별도 차감 없음.")
        st.divider()

    # ===== RESTORED FROM v3_fixed (Safe Version) =====
    # §7  대표님 보고용 한줄 요약
    sec("⑦ 📋 대표님 보고용 한줄 요약")
    if HAS_COGS:
        _sg8 = agg_kpis(df, ["sku", "asin"])
        if not _sg8.empty and "operating_profit" in _sg8.columns:
            _best_sku8  = _sg8.loc[_sg8["operating_profit"].idxmax()]
            _worst_sku8 = _sg8.loc[_sg8["operating_profit"].idxmin()]
            _best_be8   = _sg8.loc[_sg8["be_roas"].replace(np.inf, np.nan).idxmin()] if _sg8["be_roas"].replace(np.inf, np.nan).notna().any() else None
            _black_n8   = int((_sg8["operating_profit"] > 0).sum())
            _red_n8     = int((_sg8["operating_profit"] <= 0).sum())

            _best_id  = _best_sku8["sku"] if pd.notna(_best_sku8.get("sku")) else _best_sku8.get("asin","N/A")
            _worst_id = _worst_sku8["sku"] if pd.notna(_worst_sku8.get("sku")) else _worst_sku8.get("asin","N/A")
            _be_id    = (_best_be8["sku"] if pd.notna(_best_be8.get("sku")) else _best_be8.get("asin","N/A")) if _best_be8 is not None else "N/A"
            _be_val   = _best_be8["be_roas"] if _best_be8 is not None else 0

            _exec_txt = f"""**✅ 최고 수익 SKU:** {_best_id} → 영업이익 **{fu(float(_best_sku8['operating_profit']))}**

**🔴 최대 손실 SKU:** {_worst_id} → 영업이익 **{fu(float(_worst_sku8['operating_profit']))}** (즉각 점검 필요)

**📉 BE ROAS 최저 SKU (광고 효율 최고 구조):** {_be_id} → BE ROAS **{fx(_be_val) if _be_val < 900 else '∞'}**

**💡 전체 영업이익:** {fu(_PROFIT)} · 흑자 SKU {_black_n8}개 / 적자 SKU {_red_n8}개
· BE ROAS {fx(_BE_ROAS_TOT) if _BE_ROAS_TOT < 900 else '∞'} · BE ACoS {fp(_BE_ACOS_TOT)}"""
            st.info(_exec_txt)
        else:
            st.info("원가 매칭된 SKU 데이터가 부족합니다.")
    else:
        st.info("원가 데이터가 매칭되면 자동으로 표시됩니다.")
    st.divider()

    # ===== §8 현재 적용 중인 비용 가정값 현황 =====
    sec("⑧ 📊 비용 가정값 적용 현황")
    if HAS_COGS:
        # Row 1: 기본 비용
        _disp_r1 = st.columns(4)
        _disp_r1[0].metric("제품원가 조정", f"{_cost_mult_pct:+d}%")
        _disp_r1[1].metric("반품률", f"{_return_pct}%")
        _disp_r1[2].metric("3P 수수료", f"{_amazon_fee_pct}%" if _3p_on else "OFF")
        _disp_r1[3].metric("FBA 배송비", f"${_fba_ship:.1f}" if _3p_on else "OFF")
        # Row 2: 1P 비용
        if _1p_on:
            _disp_r2 = st.columns(5)
            _disp_r2[0].metric("Net PPM", f"{_net_ppm_pct}%")
            _disp_r2[1].metric("도매가 비율", f"{_wholesale_ratio*100:.1f}%")
            _disp_r2[2].metric("COOP+DA", f"{_coop_pct+_da_pct}% (DF)")
            _disp_r2[3].metric("딜 할인율", f"{_deal_pct_input}%")
            _1p_factor = (1.0 - _wholesale_ratio * (1.0 - _allowance_rate)) / max(1.0 - _deal_frac, 0.01)
            _disp_r2[4].metric("1P 채널비용", f"{_1p_factor*100:.1f}%")

        # 실제 반영 금액 분해 (1P/3P 혼합 시 평균 근사)
        _1p_allow_rate = _coop_frac + _da_frac  # FA excluded
        _1p_factor_val = (1.0 - _wholesale_ratio * (1.0 - _1p_allow_rate)) / max(1.0 - _deal_frac, 0.01)
        _1p_ded_per_u = _AVG_PRICE * _1p_factor_val if _1p_on else 0.0
        _3p_cost_per_u = (_AVG_PRICE * _fee_frac + _fba_ship) if _3p_on else 0.0
        _ret_per_u   = _AVG_PRICE * _ret_frac * 0.50
        _channel_cost_display = max(_1p_ded_per_u, _3p_cost_per_u)
        _base_cogs_u = _AVG_COGS_PU - _channel_cost_display - _ret_per_u

        _breakdown = [f"제품원가 ~{fu(_base_cogs_u)}"]
        if _1p_on: _breakdown.append(f"1P 채널비용 ~{fu(_1p_ded_per_u)} ({_1p_factor_val*100:.0f}%)")
        if _3p_on and _fee_frac > 0: _breakdown.append(f"3P 수수료 ~{fu(_AVG_PRICE * _fee_frac)}")
        if _3p_on and _fba_ship > 0: _breakdown.append(f"3P FBA ~{fu(_fba_ship)}")
        if _ret_per_u > 0: _breakdown.append(f"반품 ~{fu(_ret_per_u)}")

        st.caption(
            f"📐 **평균 단위원가 분해**: {' + '.join(_breakdown)} = "
            f"**total {fu(_AVG_COGS_PU)}/unit**  \n"
            f"💡 비용 가정값은 상단 '💰 비용 가정값 조정' 패널에서 변경하면 모든 탭에 즉시 반영됩니다."
        )
        if not _any_extra:
            st.info(
                "ℹ️ 현재 추가 비용(수수료/배송/반품/COOP/DA/FA)이 모두 0입니다.  \n"
                "unit_cogs가 ALL-IN 원가면 OK. 아니라면 상단 패널에서 조정하세요."
            )
    else:
        st.info("원가 데이터가 연결되면 비용 분해를 표시합니다.")
    st.divider()

    # ===== RESTORED FROM v3_fixed (Rewritten for canonical formula) =====
    # §9  시나리오 시뮬레이션 6종
    # profit = sales - units × unit_cogs_adj - spend (canonical formula)
    # cm_per_unit = avg_price - avg_cogs_pu (단위 공헌이익 = 판매가 - all-in 원가)
    sec("⑨ 🎯 시나리오 시뮬레이션")
    st.caption("canonical formula 기반: Operating Profit = Sales - Units × unit_cogs_adj - Spend")
    if HAS_COGS and _UNITS > 0:
        _cm_pu = _AVG_PRICE - _AVG_COGS_PU       # 단위 공헌이익 (canonical)
        _aov8 = safe_div(_SALES, _ORDERS)

        _sc1, _sc2, _sc3 = st.columns(3)
        with _sc1:
            _sim_spend_chg = st.slider(
                "A: Spend 변화 %", -50, 30, -20, 5, key="sim8_a",
                help="Spend를 줄이거나 늘릴 때 영업이익 변화")
        with _sc2:
            _sim_roas = st.slider(
                "B: 목표 ROAS", 2.0, 10.0, 5.0, 0.1, key="sim8_b",
                help="ROAS가 이 수준으로 개선되면 영업이익은?")
        with _sc3:
            _sim_cvr = st.slider(
                "D: CVR 개선 %", 5, 50, 15, 5, key="sim8_d",
                help="전환율 개선 시 효과 (리스팅/이미지 최적화)")

        # ── 6개 시나리오 계산 (canonical formula) ──
        _scenarios = []
        # 🔵 현재 상태
        _base_profit = _SALES - _UNITS * _AVG_COGS_PU - _SPEND   # == _PROFIT
        _scenarios.append({"시나리오": "🔵 현재 상태",
            "Spend": fu(_SPEND), "Sales(예상)": fu(_SALES),
            "Units(예상)": fi(_UNITS), "영업이익": round(_base_profit),
            "설명": f"현재 ROAS {fx(_ROAS)} 유지"})

        # 🅰 Spend 변화
        _ns_a = _SPEND * (1 + _sim_spend_chg / 100)
        _nu_a = _UNITS * (1 + _sim_spend_chg / 100)
        _nsl_a = _ns_a * _ROAS
        _p_a = _nsl_a - _nu_a * _AVG_COGS_PU - _ns_a
        _scenarios.append({"시나리오": f"🅰 Spend {_sim_spend_chg:+d}%",
            "Spend": fu(_ns_a), "Sales(예상)": fu(_nsl_a),
            "Units(예상)": fi(_nu_a), "영업이익": round(_p_a),
            "설명": f"광고비 {_sim_spend_chg:+d}% 조정, ROAS 유지 가정"})

        # 🅱 목표 ROAS 달성
        _nsl_b = _SPEND * _sim_roas
        _nu_b = _nsl_b / _aov8 if _aov8 > 0 else _UNITS
        _p_b = _nsl_b - _nu_b * _AVG_COGS_PU - _SPEND
        _scenarios.append({"시나리오": f"🅱 ROAS {_sim_roas:.1f} 달성",
            "Spend": fu(_SPEND), "Sales(예상)": fu(_nsl_b),
            "Units(예상)": fi(_nu_b), "영업이익": round(_p_b),
            "설명": f"같은 Spend로 ROAS {_sim_roas:.1f} 달성 시"})

        # 🅲 하위 3 SKU 중지
        _sg_sim = agg_kpis(df, ["sku", "asin"])
        if not _sg_sim.empty and "operating_profit" in _sg_sim.columns:
            _losers_c = _sg_sim.nsmallest(min(3, len(_sg_sim)), "operating_profit")
            _saved_spend_c = float(_losers_c["spend"].sum())
            _lost_units_c = float(_losers_c["units"].sum())
            _lost_sales_c = float(_losers_c["sales"].sum())
            _nu_c = _UNITS - _lost_units_c
            _nsl_c = _SALES - _lost_sales_c
            _p_c = _nsl_c - _nu_c * _AVG_COGS_PU - (_SPEND - _saved_spend_c)
            _scenarios.append({"시나리오": "🅲 하위 3 SKU 중지",
                "Spend": fu(_SPEND - _saved_spend_c), "Sales(예상)": fu(_nsl_c),
                "Units(예상)": fi(_nu_c), "영업이익": round(_p_c),
                "설명": f"적자 SKU 3개 중단 (Spend {fu(_saved_spend_c)} 절감)"})
        else:
            _scenarios.append({"시나리오": "🅲 하위 3 SKU 중지",
                "Spend": "N/A", "Sales(예상)": "N/A",
                "Units(예상)": "N/A", "영업이익": 0,
                "설명": "SKU별 데이터 부족"})

        # 🅳 CVR 개선
        _nu_d = _UNITS * (1 + _sim_cvr / 100)
        _nsl_d = _nu_d * _aov8
        _p_d = _nsl_d - _nu_d * _AVG_COGS_PU - _SPEND
        _scenarios.append({"시나리오": f"🅳 CVR +{_sim_cvr}%",
            "Spend": fu(_SPEND), "Sales(예상)": fu(_nsl_d),
            "Units(예상)": fi(_nu_d), "영업이익": round(_p_d),
            "설명": f"전환율 {_sim_cvr}% 개선 (리스팅/이미지 최적화)"})

        # 🅴 CPC 절감 10%
        _ns_e = _SPEND * 0.90
        _p_e = _SALES - _UNITS * _AVG_COGS_PU - _ns_e
        _scenarios.append({"시나리오": "🅴 CPC -10%",
            "Spend": fu(_ns_e), "Sales(예상)": fu(_SALES),
            "Units(예상)": fi(_UNITS), "영업이익": round(_p_e),
            "설명": "CPC 10% 절감 (bid 최적화), 전환 유지"})

        _sim_df8 = pd.DataFrame(_scenarios)
        st.dataframe(_sim_df8, use_container_width=True, hide_index=True)

        # 시나리오 차트
        _profits_sim8 = _sim_df8["영업이익"].tolist()
        _fig_sim8 = go.Figure()
        _fig_sim8.add_trace(go.Bar(
            x=_sim_df8["시나리오"], y=_profits_sim8,
            marker_color=["#27ae60" if p > 0 else "#e74c3c" for p in _profits_sim8],
            text=[f"${p:,.0f}" for p in _profits_sim8],
            textposition="outside"))
        _fig_sim8.add_hline(y=0, line_dash="dash", line_color="#2c3e50",
                            annotation_text="손익분기")
        _fig_sim8.update_layout(**{**BASE_LAYOUT, "height": 420,
            "title": "시나리오별 영업이익 (canonical formula 기반)"})
        st.plotly_chart(_fig_sim8, use_container_width=True)

        # 최적 시나리오 추천
        _best_idx8 = int(pd.Series(_profits_sim8).idxmax())
        _best_row8 = _sim_df8.iloc[_best_idx8]
        if _profits_sim8[_best_idx8] > 0:
            st.success(
                f"✅ **{_best_row8['시나리오']}** 시나리오가 최우선 권고입니다 "
                f"(영업이익 **${_profits_sim8[_best_idx8]:,.0f}**).  \n"
                f"→ {_best_row8['설명']}")
        else:
            st.warning(
                f"⚠️ 모든 시나리오에서 적자입니다.  \n"
                f"그나마 **{_best_row8['시나리오']}** 가 적자폭이 가장 작습니다 "
                f"(${_profits_sim8[_best_idx8]:,.0f}).  \n"
                f"→ 원가 구조 또는 판매가 조정이 근본 해결책입니다.")
    else:
        st.info("원가 데이터가 매칭되면 시나리오 시뮬레이션이 표시됩니다.")
    st.divider()

    # ── § Raw 데이터 샘플 ──────────────────────────────────────
    sec("⑥ Merged Fact 데이터 샘플")
    _show_cols8=[c for c in ["date","campaign","sku","asin","search_term","placement",
                              "spend","sales","orders","units","clicks","impressions",
                              "unit_cogs","cogs_source","operating_profit"]
                 if c in fact_merged_base.columns]
    st.dataframe(fact_merged_base[_show_cols8].head(100),use_container_width=True,hide_index=True,height=320)
    st.download_button("📥 전체 Merged Fact CSV",
                       fact_merged_base.to_csv(index=False).encode("utf-8-sig"),
                       "fact_merged.csv",key="dl_fact8")
