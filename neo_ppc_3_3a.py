"""
╔══════════════════════════════════════════════════════════════════════════╗
║  Neo PPC  v3.3a-r4  ―  Mode-Overlay + 1P/3P + Performance Fix         ║
╠══════════════════════════════════════════════════════════════════════════╣
║  [PERF] @st.fragment per tab — only active tab reruns on widget change  ║
║  [UX]   📖 차트 해석 가이드 복원 (expander)                              ║
║  [COST] 1P/3P 행 레벨 비용 모델 (v3.2.4 기반, 토글 없이 상시 적용)        ║
║  Canonical: operating_profit = sales − units × unit_cogs_adj − spend    ║
╚══════════════════════════════════════════════════════════════════════════╝
"""
import os, glob, re
from io import StringIO, BytesIO
from datetime import datetime
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

st.set_page_config(page_title="Neo PPC v3.3a", page_icon="📊",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Pretendard:wght@400;600;700&family=IBM+Plex+Mono:wght@400;600&display=swap');
html,body,[class*="css"]{font-family:'Pretendard',sans-serif;}
.kpi-card{background:#fff;border:1px solid #E2E8F0;border-radius:10px;padding:14px 16px 10px;
  box-shadow:0 1px 4px rgba(0,0,0,.05);min-height:82px;}
.kpi-card.c-blue{border-top:3px solid #3B82F6;}.kpi-card.c-green{border-top:3px solid #10B981;}
.kpi-card.c-amber{border-top:3px solid #F59E0B;}.kpi-card.c-red{border-top:3px solid #EF4444;}
.kpi-card.c-purple{border-top:3px solid #8B5CF6;}.kpi-card.c-teal{border-top:3px solid #14B8A6;}
.kpi-card.c-slate{border-top:3px solid #64748B;}
.kpi-label{font-size:10px;font-weight:700;letter-spacing:1.1px;color:#64748B;text-transform:uppercase;margin-bottom:4px;}
.kpi-value{font-size:21px;font-weight:700;color:#0F172A;font-family:'IBM Plex Mono',monospace;line-height:1.2;}
.kpi-sub{font-size:11px;color:#94A3B8;margin-top:3px;}
.kpi-good{font-size:11px;color:#16A34A;margin-top:3px;font-weight:600;}
.kpi-bad{font-size:11px;color:#DC2626;margin-top:3px;font-weight:600;}
.kpi-warn{font-size:11px;color:#D97706;margin-top:3px;font-weight:600;}
.sec-hdr{font-size:13px;font-weight:700;color:#1E293B;border-left:3px solid #3B82F6;
  padding:2px 0 2px 10px;margin:18px 0 8px;}
.mode-badge{display:inline-block;padding:4px 12px;border-radius:12px;font-size:12px;font-weight:600;}
.mode-profit{background:#DCFCE7;color:#166534;}
.mode-efficiency{background:#FEF9C3;color:#854D0E;}
.mode-scale{background:#DBEAFE;color:#1E40AF;}
[data-testid="stSidebar"]{background:#F8FAFC;}
</style>""", unsafe_allow_html=True)

# ═══════ CONSTANTS ═══════
DATA_DIR = os.path.dirname(os.path.abspath(__file__))
_COL_MAP = {"Date":"date","Campaign name":"campaign","Ad group name":"ad_group",
    "Advertised product SKU":"sku","Advertised product ID":"asin",
    "Placement classification":"placement","Target match type":"match_type",
    "Target value":"target_value","Search term":"search_term",
    "Impressions":"impressions","Clicks":"clicks","Total cost":"spend",
    "Purchases":"orders","Units sold":"units","Sales":"sales",
    "Long-term sales":"long_term_sales","Advertiser account name":"account_name"}
_SUM_COLS = ["impressions","clicks","spend","orders","units","sales","long_term_sales"]
_DEDUP_KEY = ["date","campaign","ad_group","asin","search_term","placement","target_value"]
_PL = {"Top of Search on-Amazon":"검색 상단","Other on-Amazon":"검색 기타",
    "Detail Page on-Amazon":"상품 상세","Off Amazon":"외부(Off)"}
_TS = {"close-match":"근접 매칭","loose-match":"유사 매칭","substitutes":"대체 상품",
    "complements":"보완 상품","Automatic - Close match":"근접 매칭",
    "Automatic - Loose match":"유사 매칭","Automatic - Substitutes":"대체 상품",
    "Automatic - Complements":"보완 상품"}

MODE_PROFIT="💰 Profit"; MODE_EFF="⚡ Efficiency"; MODE_SCALE="🚀 Scale"
MODES=[MODE_PROFIT,MODE_EFF,MODE_SCALE]
MODE_CFG={
    MODE_PROFIT:dict(badge="mode-profit",label="Profit (수익 우선)",
        how="ROAS vs BE_ROAS → 흑자/적자. 적자 정리 우선.",
        tabs=["📊 P&L 현황","💰 적자 진단","📈 Profit 시뮬","🏗 SKU 구조","🔧 설정"],
        sort="operating_profit",sort_asc=True),
    MODE_EFF:dict(badge="mode-efficiency",label="Efficiency (낭비 제거)",
        how="ACoS vs 목표 → 효율 판단. 낭비 검색어/입찰 최적화.",
        tabs=["📊 효율 현황","🚫 Negative","⚔ 키워드 액션","📈 CPC/CVR 최적화","🔧 설정"],
        sort="acos_pct",sort_asc=False),
    MODE_SCALE:dict(badge="mode-scale",label="Scale (성장 + 가드레일)",
        how="Sales/Units 성장 + ACoS 가드레일. 확장 발굴.",
        tabs=["📊 성장 현황","🚀 확장 캠페인","🆕 신규 발굴","📈 예산 시뮬","🔧 설정"],
        sort="sales",sort_asc=False),
}
C=dict(spend="#3B82F6",sales="#10B981",acos="#F59E0B",roas="#8B5CF6",
    cpc="#EF4444",row_good="#F0FDF4",row_warn="#FEFCE8",row_bad="#FEF2F2")
BL=dict(plot_bgcolor="#FFF",paper_bgcolor="#F8FAFC",
    font=dict(color="#374151",size=11,family="Pretendard"),hovermode="x unified",
    legend=dict(orientation="v",x=1.01,y=1,xanchor="left",font=dict(size=11),
        bgcolor="rgba(255,255,255,.85)",bordercolor="#E2E8F0",borderwidth=1),
    margin=dict(l=4,r=140,t=40,b=4),
    xaxis=dict(gridcolor="#E5E7EB",zeroline=False),yaxis=dict(gridcolor="#E5E7EB",zeroline=False))

# ═══════ HELPERS ═══════
def fu(v): return f"${v:,.2f}" if pd.notna(v) else "$0.00"
def fp(v): return f"{v:.1f}%" if pd.notna(v) else "0.0%"
def fx(v): return f"{v:.2f}x" if pd.notna(v) else "0.00x"
def fi(v): return f"{int(v):,}" if pd.notna(v) else "0"
def sd(n,d,pct=False):
    try:
        if d==0 or pd.isna(d) or pd.isna(n): return 0.0
        r=float(n)/float(d); return r*100. if pct else r
    except: return 0.0
def sec(t): st.markdown(f'<div class="sec-hdr">{t}</div>',unsafe_allow_html=True)
def cg(md):
    """Chart guide expander"""
    with st.expander("📖 차트 해석 가이드",expanded=False): st.markdown(md)
def kc(col,label,value,sub="",sub_cls="kpi-sub",color="blue"):
    sh=f'<div class="{sub_cls}">{sub}</div>' if sub else ""
    col.markdown(f'<div class="kpi-card c-{color}"><div class="kpi-label">{label}</div>'
                 f'<div class="kpi-value">{value}</div>{sh}</div>',unsafe_allow_html=True)

# ═══════ VERDICT ═══════
def vd_profit(r,be):
    ro=r.get("roas",0)
    if r.get("structural_loss",False) or be>=900: return "🔴 CUT"
    if ro>=be*1.2: return "🟢 SCALE"
    if ro>=be*0.9: return "🟡 HOLD"
    if ro>=be*0.7: return "🔧 FIX"
    return "🔴 CUT"
def vd_eff(r,ta,mc=5):
    ac=r.get("acos_pct",999)
    if r.get("orders",0)==0 and r.get("clicks",0)>=mc: return "🔴 CUT"
    if ac<=ta: return "🟢 SCALE"
    if ac<=ta*1.3: return "🟡 HOLD"
    if ac<=ta*2.0: return "🔧 FIX"
    return "🔴 CUT"
def vd_scale(r,ta):
    ac=r.get("acos_pct",999)
    if ac<=ta and r.get("sales",0)>0: return "🟢 SCALE"
    if ac<=ta*1.5: return "🟡 HOLD"
    if ac<=ta*2.5: return "🔧 FIX"
    return "🔴 CUT"
def av(r,m,be,ta,mc=5):
    if m==MODE_PROFIT: return vd_profit(r,be)
    elif m==MODE_EFF: return vd_eff(r,ta,mc)
    return vd_scale(r,ta)
def tlc(v):
    if "SCALE" in v: return C["row_good"]
    if "HOLD" in v: return C["row_warn"]
    return C["row_bad"]
def _ckw(t):
    t=str(t).lower()
    for b in ["neochair","neo chair","neo sleep","mlw","801","nec","pace","blc","titan","apex","czpu","czw","hpac","cub","cntt","cozy","mesh801"]:
        if b in t: return "🏷 Brand"
    for c in ["hbada","sihoo","secretlab","autonomous","branch","herman miller","steelcase","haworth","ergochair","dxracer","respawn","nouhaus"]:
        if c in t: return "⚔ Competitor"
    return "🔍 Generic"
def _ctg(v):
    if pd.isna(v) or str(v).strip()=="": return "수동"
    return _TS.get(str(v).strip(),"수동")
def _cpur(n):
    n=str(n).lower()
    if "brand" in n: return "🏷 Brand"
    if any(x in n for x in ["keyword","manual","exact"]): return "🎯 Manual"
    if "b2b" in n: return "🏢 B2B"
    if "auto" in n: return "🤖 Auto"
    return "📦 기타"
def _tier(ro,pr,od,sp,avg,be):
    if od==0 and sp>0: return "T4 🚫 차단"
    if ro>=be*1.2 and pr>0 and od>=2 and sp>=avg*0.5: return "T1 ⭐ 확장"
    if ro<be*0.8 and pr<0: return "T3 🔴 적자"
    return "T2 🟡 유지"

# ═══════ EXTRACT ═══════
@st.cache_data(show_spinner="📊 Ads 로드…")
def load_ads(raw:bytes)->pd.DataFrame:
    df=pd.read_csv(StringIO(raw.decode("utf-8-sig")),low_memory=False)
    df.columns=df.columns.str.strip()
    for c in ["Impressions","Clicks","Total cost","Purchases","Units sold","Sales","Long-term sales"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce").fillna(0.)
    if "Date" in df.columns:
        df["Date"]=pd.to_datetime(df["Date"],format="%b %d, %Y",errors="coerce")
        df.dropna(subset=["Date"],inplace=True)
    df=df.rename(columns={k:v for k,v in _COL_MAP.items() if k in df.columns})
    df["campaign_type"]=np.where(df["campaign"].str.contains("Auto",case=False,na=False),"Auto","Manual")
    if "placement" in df.columns: df["placement"]=df["placement"].map(_PL).fillna(df["placement"])
    df["target_group"]=df.get("target_value",pd.Series(dtype=str)).apply(_ctg) if "target_value" in df.columns else "수동"
    for c in _SUM_COLS+["asin","sku","search_term","placement","ad_group","target_value","long_term_sales"]:
        if c not in df.columns: df[c]=np.nan if c not in _SUM_COLS else 0.
    df["asin"]=df["asin"].astype(str).str.strip().str.upper().replace("NAN","")
    df["sku"]=df["sku"].astype(str).str.strip().str.upper().replace("NAN","")
    df.loc[df["asin"]=="","asin"]=np.nan; df.loc[df["sku"]=="","sku"]=np.nan
    am=df["sku"].str.startswith("AMZN",na=False); df.loc[am,"sku"]=df.loc[am,"asin"]
    if "account_name" in df.columns: df["account_name"]=df["account_name"].fillna("Unknown")
    hc="campaign" in df.columns and df["campaign"].notna().any()
    df["sales_channel"]=np.where(df["campaign"].str.startswith("1P",na=False),"1P","3P") if hc else "3P"
    key=[c for c in _DEDUP_KEY if c in df.columns]
    return df.drop_duplicates(subset=key).reset_index(drop=True)

@st.cache_data(show_spinner="🏷 원가 로드…")
def load_cogs(raw:bytes,ext:str)->pd.DataFrame:
    if ext in ("xlsx","xls"): r=pd.read_excel(BytesIO(raw))
    else: r=pd.read_csv(StringIO(raw.decode("utf-8-sig")))
    r.columns=r.columns.str.strip(); lo={c.lower():c for c in r.columns}
    ao=lo.get("asin") or next((v for k,v in lo.items() if "asin" in k),None)
    co=lo.get("원가") or lo.get("unit_cogs") or lo.get("cogs") or next((v for k,v in lo.items() if "원가" in k or "cogs" in k),None)
    if not ao or not co: st.warning("⚠ ASIN/원가 컬럼 미발견"); return pd.DataFrame(columns=["asin","unit_cogs"])
    df=r[[ao,co]].copy(); df.columns=["asin","unit_cogs"]
    df["asin"]=df["asin"].astype(str).str.strip().str.upper()
    df["unit_cogs"]=pd.to_numeric(df["unit_cogs"],errors="coerce")
    df=df[df["unit_cogs"].notna()&(df["asin"]!="")&(df["asin"]!="NAN")]
    return df.sort_values("unit_cogs",ascending=False).drop_duplicates("asin",keep="first").reset_index(drop=True)

def _find_cogs(d):
    for p in ["*unit_cogs*.xlsx","*cogs*.xlsx","*원가*.xlsx"]:
        h=glob.glob(os.path.join(d,p))
        if h: return max(h,key=os.path.getmtime)
def _find_ads(d):
    h=[]
    for p in ["PPC_analysis*.csv","*PPC*.csv"]: h.extend(glob.glob(os.path.join(d,p)))
    return sorted(set(h),key=os.path.getmtime,reverse=True)

# ═══════ TRANSFORM ═══════
@st.cache_data(show_spinner="🔗 병합…")
def merge_cogs(fact,cogs):
    dr=[c for c in ["unit_cogs","cogs_source"] if c in fact.columns]
    f=fact.drop(columns=dr).copy() if dr else fact.copy()
    if cogs.empty: f["unit_cogs"]=np.nan; f["cogs_source"]="proxy"; return f
    m=f.merge(cogs[["asin","unit_cogs"]],on="asin",how="left")
    m["cogs_source"]=np.where(m["unit_cogs"].notna(),"actual","proxy"); return m

def add_profit(df,cm,fee=0.,fba=0.,ret=0.,coop=0.,da=0.,fa=0.,wr=0.82,dp=0.10):
    """1P/3P row-level cost → canonical profit."""
    d=df.copy()
    d["_apr"]=np.where(d["units"]>0,d["sales"]/d["units"],0.)
    fb=d["_apr"]*0.70
    bc=np.where(d["unit_cogs"].notna(),d["unit_cogs"]*(1+cm),fb*(1+cm))
    is1=(d["sales_channel"]=="1P").values if "sales_channel" in d.columns else np.zeros(len(d),dtype=bool)
    oa=coop+da; dn=max(1.-dp,0.01)
    od=d["_apr"]*(1.-wr*(1.-oa))/dn
    tc=d["_apr"]*fee+fba
    cc=np.where(is1,od,tc)
    rc=d["_apr"]*ret*0.50
    d["unit_cogs_adj"]=bc+cc+rc
    d["is_proxy_cost"]=d["unit_cogs"].isna()
    d["operating_profit"]=d["sales"]-d["units"]*d["unit_cogs_adj"]-d["spend"]
    d["be_spend"]=d["sales"]-d["units"]*d["unit_cogs_adj"]
    d["be_roas"]=np.where(d["be_spend"]>0,d["sales"]/d["be_spend"],np.inf)
    d["be_acos"]=np.where((d["be_spend"]>0)&(d["sales"]>0),d["be_spend"]/d["sales"]*100,np.nan)
    d["structural_loss"]=d["be_spend"]<=0
    d.drop(columns=["_apr"],inplace=True,errors="ignore"); return d

# ═══════ AGG ═══════
def rr(g):
    sp=g["spend"];sa=g["sales"];cl=g["clicks"];im=g["impressions"];o=g["orders"];u=g["units"]
    g["roas"]=sa.div(sp.replace(0,np.nan)).fillna(0)
    g["acos_pct"]=sp.div(sa.replace(0,np.nan)).fillna(0)*100
    g["cpc"]=sp.div(cl.replace(0,np.nan)).fillna(0)
    g["ctr_pct"]=cl.div(im.replace(0,np.nan)).fillna(0)*100
    g["cvr_pct"]=o.div(cl.replace(0,np.nan)).fillna(0)*100
    g["aov"]=sa.div(o.replace(0,np.nan)).fillna(0)
    g["avg_price"]=sa.div(u.replace(0,np.nan)).fillna(0)
    g["cpa"]=sp.div(o.replace(0,np.nan)).fillna(0)
    g["cpa_unit"]=sp.div(u.replace(0,np.nan)).fillna(0)
    if "be_spend" in g.columns:
        g["be_roas"]=np.where(g["be_spend"]>0,g["sales"]/g["be_spend"],np.inf)
        g["be_acos"]=np.where((g["be_spend"]>0)&(g["sales"]>0),g["be_spend"]/g["sales"]*100,np.nan)
        g["structural_loss"]=g["be_spend"]<=0
    return g

def ag(df,gc):
    ex=["unit_cogs_adj","operating_profit","is_proxy_cost","be_spend","long_term_sales"]
    am={c:"sum" for c in _SUM_COLS+ex if c in df.columns}
    return rr(df.groupby(gc,dropna=False).agg(am).reset_index())

@st.cache_data(show_spinner=False)
def pre_agg(h,df):
    ac=ag(df,["campaign","campaign_type"])
    ask=ag(df,["sku","asin"])
    akw=ag(df,["search_term","campaign"])
    apl=ag(df,["placement"]) if df["placement"].notna().any() else pd.DataFrame()
    atg=ag(df,["target_group"]) if "target_group" in df.columns else pd.DataFrame()
    return ac,ask,akw,apl,atg

def cov(df):
    if "unit_cogs" not in df.columns: return dict(tot=0,mat=0,pct=0.,sp=0.)
    h=df[df["asin"].notna()&(df["asin"]!="")]
    t=h["asin"].nunique(); m=h[h["unit_cogs"].notna()]["asin"].nunique()
    ts=h["spend"].sum(); ms=h[h["unit_cogs"].notna()]["spend"].sum()
    return dict(tot=t,mat=m,pct=sd(m,t,True),sp=sd(ms,ts,True))

# ═══════ CHART ═══════
def blf(x,bars,lk=None,title="",h=300):
    us=lk is not None
    fig=make_subplots(specs=[[{"secondary_y":us}]]) if us else go.Figure()
    for n,(y,c) in bars.items():
        if us and n==lk:
            fig.add_trace(go.Scatter(x=x,y=y,name=n,mode="lines+markers",line=dict(color=c,width=2.5)),secondary_y=True)
        else:
            tr=go.Bar(x=x,y=y,name=n,marker_color=c,opacity=.85,text=y.map(lambda v:f"${v:,.0f}"),textposition="outside",textfont=dict(size=9))
            fig.add_trace(tr,secondary_y=False) if us else fig.add_trace(tr)
    fig.update_layout(**{**BL,"barmode":"group","height":h,"title":title}); return fig

def vtf(c2,m,ta,be):
    """Verdict table figure."""
    colors=[tlc(av(c2.iloc[i],m,be,ta)) for i in range(len(c2))]
    cols=["campaign","campaign_type","spend","sales","orders","units","acos_pct","roas","cpc","ctr_pct","cvr_pct","cpa","cpa_unit"]
    if m==MODE_PROFIT and "operating_profit" in c2.columns: cols.append("operating_profit")
    cols=[c for c in cols if c in c2.columns]
    lb={"campaign":"캠페인","campaign_type":"유형","spend":"Spend($)","sales":"Sales($)",
        "orders":"주문","units":"판매","acos_pct":"ACoS(%)","roas":"ROAS(x)","cpc":"CPC($)",
        "ctr_pct":"CTR(%)","cvr_pct":"CVR(%)","cpa":"CPA($)","cpa_unit":"CPA/u($)","operating_profit":"영업이익($)"}
    fm={"spend":fu,"sales":fu,"acos_pct":fp,"roas":fx,"cpc":fu,"ctr_pct":fp,"cvr_pct":fp,
        "cpa":fu,"cpa_unit":fu,"orders":fi,"units":fi,"operating_profit":fu}
    vals=[c2[c].map(fm[c]) if c in fm else c2[c] for c in cols]
    fig=go.Figure(go.Table(
        columnwidth=[160]+[50]*(len(cols)-1),
        header=dict(values=[f"<b>{lb.get(c,c)}</b>" for c in cols],fill_color="#1E293B",font=dict(color="white",size=10),align="center",height=30),
        cells=dict(values=vals,fill_color=[colors]*len(cols),font=dict(color="#1E293B",size=10),align="center",height=26)))
    fig.update_layout(height=max(220,len(c2)*28+80),margin=dict(l=0,r=0,t=4,b=0)); return fig

# ═══════════════════════════════════════════
#  SIDEBAR + DATA LOAD + GLOBAL KPIs
# ═══════════════════════════════════════════
with st.sidebar:
    st.markdown("### 📊 Neo PPC v3.3a")
    st.divider()
    st.markdown("**🎯 모드**")
    if "ppc_mode" not in st.session_state: st.session_state["ppc_mode"]=MODE_PROFIT
    mi=MODES.index(st.session_state["ppc_mode"]) if st.session_state["ppc_mode"] in MODES else 0
    PM=st.radio("",MODES,index=mi,label_visibility="collapsed",key="_mr")
    st.session_state["ppc_mode"]=PM; mc=MODE_CFG[PM]
    st.markdown(f'<span class="mode-badge {mc["badge"]}">{mc["label"]}</span>',unsafe_allow_html=True)
    st.caption(mc["how"])
    st.divider()
    up_ads=st.file_uploader("Ads CSV",type=["csv"],accept_multiple_files=True,label_visibility="collapsed",key="ua")
    up_cogs=st.file_uploader("원가 마스터",type=["csv","xlsx","xls"],label_visibility="collapsed",key="uc")
    st.divider()

ff=[]
if up_ads:
    for f in up_ads:
        try: ff.append(load_ads(f.read()))
        except Exception as e: st.sidebar.error(f"❌ {f.name}: {e}")
else:
    for fp2 in _find_ads(DATA_DIR):
        try:
            with open(fp2,"rb") as fh: ff.append(load_ads(fh.read()))
            st.sidebar.caption(f"📄 {os.path.basename(fp2)}")
        except: pass
if not ff: st.info("👈 Ads CSV 업로드"); st.stop()

fr=pd.concat(ff,ignore_index=True)
fr["date"]=pd.to_datetime(fr["date"],errors="coerce")
fr=fr.dropna(subset=["date"]).drop_duplicates(subset=[c for c in _DEDUP_KEY if c in fr.columns]).reset_index(drop=True)

cd=pd.DataFrame(columns=["asin","unit_cogs"])
if up_cogs:
    ext=up_cogs.name.rsplit(".",1)[-1].lower()
    try: cd=load_cogs(up_cogs.read(),ext); st.sidebar.success(f"✅ 원가 {len(cd)}")
    except Exception as e: st.sidebar.warning(f"⚠ {e}")
else:
    cp=_find_cogs(DATA_DIR)
    if cp:
        ext=cp.rsplit(".",1)[-1].lower()
        try:
            with open(cp,"rb") as fh: cd=load_cogs(fh.read(),ext)
            st.sidebar.caption(f"🏷 {os.path.basename(cp)}")
        except: pass

HC=not cd.empty
fm=merge_cogs(fr,cd)
cv=cov(fm)
if HC:
    ic="✅" if cv["pct"]>=80 else("⚠" if cv["pct"]>=50 else "❌")
    st.sidebar.caption(f"💰 매칭: {ic} {cv['mat']}/{cv['tot']} ({cv['pct']:.0f}%)")

# ── Filters ──
with st.sidebar:
    st.markdown("**📅 기간**")
    ds=fr["date"].dropna()
    if ds.empty: st.error("❌"); st.stop()
    dmn=ds.min().date(); dmx=ds.max().date()
    dr=st.date_input("",value=(dmn,dmx),min_value=dmn,max_value=dmx,label_visibility="collapsed",key="gd")
    D0=pd.Timestamp(dr[0]) if len(dr)>=1 else pd.Timestamp(dmn)
    D1=pd.Timestamp(dr[1]) if len(dr)==2 else pd.Timestamp(dmx)
    sel_acct=None
    if "account_name" in fm.columns and fm["account_name"].nunique()>1:
        sel_acct=st.multiselect("계정",sorted(fm["account_name"].dropna().unique()),
            default=sorted(fm["account_name"].dropna().unique()),key="ga")
    sel_type=st.multiselect("유형",sorted(fm["campaign_type"].dropna().unique()),
        default=sorted(fm["campaign_type"].dropna().unique()),key="gt")
    sel_camp=st.multiselect("캠페인",sorted(fm["campaign"].dropna().unique()),default=[],placeholder="전체",key="gc")
    st.divider()
    TA=st.number_input("목표 ACoS(%)",value=15.0,min_value=5.0,max_value=60.0,step=1.0,key="ta")
    TW=TA*1.5
    MC=st.number_input("최소 클릭",value=5,min_value=1,step=1,key="gmc")
    st.divider()

    # ── 📦 비용 설정 ──
    st.markdown("##### 📦 기본 비용")
    ba1,ba2=st.columns(2)
    with ba1: _cmp=st.slider("원가 조정(%)",-30,50,0,1,format="%d%%",key="cm",help="unit_cogs 일괄 조정")
    with ba2: _rp=st.slider("↩️ 반품률(%)",0,30,0,1,format="%d%%",key="rp",help="반품시 판매가 50% 손실")
    st.markdown("---")
    st.markdown("##### 🛒 3P 비용")
    st.caption("'1P' 외 모든 행에 적용")
    t1,t2=st.columns(2)
    with t1: _afp=st.slider("수수료(%)",0,25,15,1,format="%d%%",key="af")
    with t2: _fba=st.number_input("FBA($/u)",value=11.0,min_value=0.0,max_value=30.0,step=0.5,key="fb")
    st.markdown("---")
    st.markdown("##### 🏢 1P 비용 (DF)")
    st.caption("캠페인 '1P*' 행에만 적용. FA 제외.")
    p1,p2=st.columns(2)
    with p1: _npm=st.slider("Net PPM(%)",15,45,27,1,format="%d%%",key="np")
    with p2: _dpct=st.slider("딜 할인(%)",0,30,10,1,format="%d%%",key="dp")
    p3,p4=st.columns(2)
    with p3: _cop=st.slider("COOP(%)",0,15,8,1,format="%d%%",key="co")
    with p4: _dap=st.slider("DA(%)",0,10,3,1,format="%d%%",key="da")

    CM=_cmp/100.; FF=_afp/100.; RF=_rp/100.; CF=_cop/100.; DF=_dap/100.; DPF=_dpct/100.; NPF=_npm/100.
    AR=CF+DF; WR=(1.-NPF)/max(1.-AR,0.01); F1P=(1.-WR*(1.-AR))/max(1.-DPF,0.01)
    st.caption(f"도매가 비율 {WR*100:.1f}% | 1P factor {F1P*100:.1f}%")
    st.divider()

# ── Filter + Profit ──
mk=(fm["date"]>=D0)&(fm["date"]<=D1)
if sel_type: mk&=fm["campaign_type"].isin(sel_type)
if sel_camp: mk&=fm["campaign"].isin(sel_camp)
if sel_acct: mk&=fm["account_name"].isin(sel_acct)
dfr=fm[mk].copy()
if dfr.empty: st.warning("⚠ 데이터 없음"); st.stop()

df=add_profit(dfr,CM,fee=FF,fba=_fba,ret=RF,coop=CF,da=DF,fa=0.,wr=WR,dp=DPF)

# ── Global KPIs ──
S=float(df["spend"].sum()); SA=float(df["sales"].sum())
OR=float(df["orders"].sum()); UN=float(df["units"].sum())
CL=float(df["clicks"].sum()); IM=float(df["impressions"].sum())
PR=float(df["operating_profit"].sum())
RO=sd(SA,S); AC=sd(S,SA,True); CP=sd(S,CL); CT=sd(CL,IM,True)
CV=sd(OR,CL,True); AO=sd(SA,OR); AP=sd(SA,UN); CA=sd(S,OR); CU=sd(S,UN)
TC=float((df["units"]*df["unit_cogs_adj"]).sum())
BS=SA-TC; BR=(SA/BS) if BS>0 else 999.; BA=(BS/SA*100) if (BS>0 and SA>0) else 0.; ACU=TC/max(UN,1)

# ── Pre-compute aggs (cached — mode-independent) ──
dh=hash((pd.util.hash_pandas_object(df[["spend","sales"]]).sum(),CM,FF,RF,CF,DF,DPF,_fba))
AC2,ASK,AKW,APL,ATG=pre_agg(dh,df)

# ── Header ──
st.markdown(f'<span class="mode-badge {mc["badge"]}">{PM}</span> <span style="font-size:12px;color:#64748B">{mc["how"]}</span>',unsafe_allow_html=True)
r1=st.columns(5)
kc(r1[0],"Spend",fu(S),color="blue"); kc(r1[1],"Sales",fu(SA),f"ROAS {fx(RO)}",color="green")
ac_c="kpi-good" if AC<=TA else("kpi-warn" if AC<=TW else "kpi-bad")
kc(r1[2],"ACoS",fp(AC),f"목표 {TA:.0f}%",sub_cls=ac_c,color="amber")
kc(r1[3],"CPA",fu(CA),f"CPA/u {fu(CU)}",color="red" if CA>AO else "blue")
if HC: kc(r1[4],"영업이익",fu(PR),"🟢 흑자" if PR>0 else "🔴 적자",sub_cls="kpi-good" if PR>0 else "kpi-bad",color="green" if PR>0 else "red")
else: kc(r1[4],"CTR",fp(CT),f"CVR {fp(CV)}",color="slate")
r2=st.columns(5)
kc(r2[0],"노출",fi(IM),f"CTR {fp(CT)}",color="slate"); kc(r2[1],"클릭",fi(CL),f"CPC {fu(CP)}",color="slate")
kc(r2[2],"CVR",fp(CV),f"주문 {fi(OR)}",color="purple"); kc(r2[3],"AOV",fu(AO),f"단가 {fu(AP)}",color="teal")
if HC and BR<900: kc(r2[4],"BE ROAS",fx(BR),f"BE ACoS {fp(BA)}",color="purple")
else: kc(r2[4],"주문/노출",fp(sd(OR,IM,True)),color="slate")
st.caption(f"{D0.date()}~{D1.date()} | 캠페인 {df['campaign'].nunique()} | ASIN {df['asin'].nunique()}")

with st.expander("📐 영업이익 공식 (1P/3P)",expanded=False):
    ea=CF+DF
    ef=(1.-WR*(1.-ea))/max(1.-DPF,0.01)
    st.code(
        "── 공통 ──\n"
        f"base_cogs   = unit_cogs × (1 + {CM:+.0%})\n"
        f"return_cost = avg_price × {RF:.2f} × 0.50\n"
        f"\n── 1P (campaign '1P*', DF) ──\n"
        f"Net PPM={NPF:.2f} → wholesale_ratio={WR:.4f}\n"
        f"allowances = COOP {CF:.2f} + DA {DF:.2f}\n"
        f"factor = {ef:.4f} ({ef*100:.1f}%)\n"
        f"channel_deduction = avg_price × {ef:.4f}\n"
        f"\n── 3P (나머지) ──\n"
        f"channel_cost = avg_price × {FF:.2f} + ${_fba:.1f}\n"
        "\n── 최종 ──\n"
        "operating_profit = sales − units × unit_cogs_adj − spend\n"
        "be_roas = sales / be_spend",language="")
st.divider()

# ═══════════════════════════════════════════
#  MODE-SPECIFIC TABS (each tab = @st.fragment)
# ═══════════════════════════════════════════

tabs=st.tabs(mc["tabs"])

# ── Verdict (lazy, uses pre-agg) ──
def _vcamp():
    AC2["verdict"]=AC2.apply(lambda r:av(r,PM,BR,TA,MC),axis=1); return AC2
def _vsku():
    ASK["verdict"]=ASK.apply(lambda r:av(r,PM,BR,TA,MC),axis=1)
    asp=ASK["spend"].mean() if len(ASK)>0 else 1
    ASK["tier"]=ASK.apply(lambda r:_tier(r["roas"],r.get("operating_profit",0),r["orders"],r["spend"],asp,BR),axis=1)
    return ASK

# ───────────────── PROFIT MODE ─────────────────
if PM==MODE_PROFIT:

    with tabs[0]:
        @st.fragment
        def profit_tab1():
            sec("📈 주간 트렌드")
            cg("**W-1 vs W-2 비교:** 전주 대비 개선/악화를 🟢🔴로 표시.\n"
               "ACoS 상승+Sales 하락 → 효율 악화. 반대면 양호.")
            wk=df.copy(); wk["week"]=wk["date"].dt.to_period("W").apply(lambda p:p.start_time)
            wa=wk.groupby("week").agg({c:"sum" for c in _SUM_COLS if c in wk.columns}).reset_index()
            wa=rr(wa); wa=wa.sort_values("week").tail(6); wa["ws"]=wa["week"].dt.strftime("%m/%d")
            if len(wa)>=2:
                wl=wa.iloc[-1]; wp=wa.iloc[-2]
                tm=[("spend","Spend",fu,"n"),("sales","Sales",fu,"u"),("acos_pct","ACoS%",fp,"d"),
                    ("roas","ROAS",fx,"u"),("ctr_pct","CTR%",fp,"u"),("cvr_pct","CVR%",fp,"u"),
                    ("cpa","CPA",fu,"d"),("cpa_unit","CPA/u",fu,"d")]
                dc=st.columns(len(tm))
                for i,(col,lb,fmt,pol) in enumerate(tm):
                    if col not in wa.columns: continue
                    vn=wl.get(col,0); vp=wp.get(col,0); d=vn-vp
                    if pol=="u": ic="🟢" if d>0 else("🔴" if d<0 else "🟡")
                    elif pol=="d": ic="🟢" if d<0 else("🔴" if d>0 else "🟡")
                    else: ic="⚪"
                    dc[i].metric(lb,fmt(vn),f"{ic} {d:+.1f}" if isinstance(d,float) else "")
                fwk=make_subplots(specs=[[{"secondary_y":True}]])
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["spend"],name="Spend",marker_color=C["spend"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["sales"],name="Sales",marker_color=C["sales"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Scatter(x=wa["ws"],y=wa["acos_pct"],name="ACoS%",mode="lines+markers",line=dict(color=C["acos"],width=2.5)),secondary_y=True)
                fwk.add_hline(y=TA,line_dash="dash",line_color="#EF4444",secondary_y=True)
                fwk.update_layout(**{**BL,"barmode":"group","height":280,"title":"주간 트렌드"})
                st.plotly_chart(fwk,use_container_width=True)
            st.divider()
            sec("📊 캠페인 P&L")
            cg("**4단계 판정:**\n- 🟢 SCALE: ROAS≥BE×1.2 → 확대\n- 🟡 HOLD: BE×0.9~1.2 → 유지\n"
               "- 🔧 FIX: BE×0.7~0.9 → 최적화\n- 🔴 CUT: <BE×0.7 → Pause\n\n**핵심:** CUT 많으면 구조 문제.")
            vc=_vcamp()
            ns=(vc["verdict"].str.contains("SCALE")).sum(); nc=(vc["verdict"].str.contains("CUT")).sum()
            lo=vc[vc.get("operating_profit",pd.Series(0))<0] if "operating_profit" in vc.columns else pd.DataFrame()
            nl=len(lo); ll=float(lo["operating_profit"].sum()) if not lo.empty else 0
            v1,v2,v3,v4=st.columns(4)
            kc(v1,"🟢 SCALE",str(ns),color="green"); kc(v2,"🟡 HOLD",str((vc["verdict"].str.contains("HOLD")).sum()),color="amber")
            kc(v3,"🔧 FIX",str((vc["verdict"].str.contains("FIX")).sum()),color="blue")
            kc(v4,"🔴 CUT",str(nc),f"적자 {nl}개 ({fu(abs(ll))})",color="red")
            sm=mc["sort"] if mc["sort"] in vc.columns else "spend"
            vc2=vc.sort_values(sm,ascending=mc.get("sort_asc",False)).reset_index(drop=True)
            st.plotly_chart(vtf(vc2,PM,TA,BR),use_container_width=True)
            st.download_button("📥 캠페인 CSV",vc2.to_csv(index=False).encode("utf-8-sig"),"camp.csv",key="dc1")
            st.divider()
            sec("🔍 드릴다운")
            cg("캠페인 선택 → SKU/ASIN별 성과. Spend 높은 SKU부터 확인.")
            dd=st.selectbox("캠페인",["전체"]+sorted(df["campaign"].dropna().unique().tolist()),key="dd1")
            if dd!="전체":
                ddf=df[df["campaign"]==dd]; dda=ag(ddf,["sku","asin"])
                dda["display"]=dda["sku"].fillna(dda["asin"])
                cs=["display","spend","sales","orders","units","acos_pct","roas","cpc","cvr_pct","cpa","cpa_unit"]
                if "operating_profit" in dda.columns: cs.append("operating_profit")
                cs=[c for c in cs if c in dda.columns]
                st.dataframe(dda[cs].sort_values("spend",ascending=False),use_container_width=True,hide_index=True,height=280)
        profit_tab1()

    with tabs[1]:
        @st.fragment
        def profit_tab2():
            sec("① 적자 캠페인 Pause")
            cg("하위 N개 적자 캠페인 중단 시 이익/매출 트레이드오프.\n"
               "**결정 기준:** Pause후 영업이익 증가 → 실행. 매출 감소가 크면 → 부분 Bid↓ 먼저.")
            vc=_vcamp()
            if "operating_profit" in vc.columns:
                tl=int((vc["operating_profit"]<0).sum())
                if tl==0: st.success("✅ 적자 없음")
                else:
                    pn=st.slider("Pause 수",1,max(tl,1),min(3,tl),key="pp")
                    w=vc.nsmallest(pn,"operating_profit")
                    ps=float(w["spend"].sum()); psa=float(w["sales"].sum())
                    as2=S-ps; asa=SA-psa; ar=sd(asa,as2); aop=PR-float(w["operating_profit"].sum())
                    st.dataframe(pd.DataFrame({"항목":["Spend","Sales","ROAS","영업이익"],
                        "현재":[fu(S),fu(SA),fx(RO),fu(PR)],"Pause후":[fu(as2),fu(asa),fx(ar),fu(aop)],
                        "변화":[fu(-ps),fu(-psa),f"{ar-RO:+.2f}x",f"${aop-PR:+,.0f}"]}),use_container_width=True,hide_index=True)
                    if aop>PR: st.success(f"✅ +${aop-PR:,.0f}")
                    st.dataframe(w[["campaign","spend","sales","operating_profit","roas","acos_pct"]],use_container_width=True,hide_index=True)
            st.divider()
            sec("② Spend 절감 (worst ACoS)")
            cg("ACoS 높은 캠페인부터 절감. **가정:** 절감 비례로 Sales도 감소(ROAS 불변).")
            cp2=st.slider("절감(%)",5,50,20,5,key="pc")
            sw=vc.sort_values("acos_pct",ascending=False) if "acos_pct" in vc.columns else vc
            ca2=S*cp2/100; rem=ca2; cc=[]
            for _,r in sw.iterrows():
                if rem<=0: break
                cs2=float(r["spend"]); ac2=min(cs2,rem)
                cc.append({"캠페인":r["campaign"],"Spend":fu(cs2),"절감":fu(ac2),"ACoS":fp(r.get("acos_pct",0))}); rem-=ac2
            if cc: st.dataframe(pd.DataFrame(cc),use_container_width=True,hide_index=True)
            ns2=S*(1-cp2/100); es=ns2*RO; ep=es-(UN*(1-cp2/100))*ACU-ns2
            st.metric("예상 영업이익",fu(ep),f"${ep-PR:+,.0f}")
            st.divider()
            sec("③ 예산 재배분 (ROAS 비례)")
            cg("총 Spend 유지 → ROAS 비례 배분. 고효율 캠페인에 집중 투자.")
            rl=vc.copy(); rs=rl["roas"].sum()
            rl["roas_w"]=rl["roas"]/rs if rs>0 else 1/max(len(rl),1)
            rl["new_spend"]=S*rl["roas_w"]; rl["new_sales"]=rl["new_spend"]*rl["roas"]
            rl["new_units"]=(rl["new_sales"]/AO) if AO>0 else 0
            rl["new_op"]=rl["new_sales"]-rl["new_units"]*ACU-rl["new_spend"]
            nop=float(rl["new_op"].sum())
            m1,m2=st.columns(2)
            m1.metric("현재",fu(PR)); m2.metric("재배분 후",fu(nop),f"${nop-PR:+,.0f}")
        profit_tab2()

    with tabs[2]:
        @st.fragment
        def profit_tab3():
            sec("① Incrementality")
            cg("광고 완전 중단 시 잔존 매출. **Incrementality=0.80** → 광고 기여 80%, 자연 매출 20%.")
            inc=st.slider("Incrementality",0.40,1.00,0.80,0.05,key="pi")
            or2=1.-inc; os2=SA*or2; ou=UN*or2; oop=os2-ou*ACU
            st.dataframe(pd.DataFrame([{"시나리오":"현재","Spend":fu(S),"Sales":fu(SA),"영업이익":fu(PR)},
                {"시나리오":"광고 중단","Spend":"$0","Sales":fu(os2),"영업이익":fu(oop)}]),use_container_width=True,hide_index=True)
            st.divider()
            sec("② Spend 증감")
            cg("ROAS 불변 가정. Spend ±% → Sales/이익 변화. **한계:** 실제론 Spend↑→ROAS↓ 체감.")
            ss=[]
            for p in [-30,-20,-10,0,10,20,30]:
                ns=S*(1+p/100); nsa=ns*RO; nu=UN*(1+p/100); nop=nsa-nu*ACU-ns
                ss.append({"변화":f"{p:+d}%","Spend":fu(ns),"Sales":fu(nsa),"영업이익":fu(nop),"":("← 현재" if p==0 else "")})
            st.dataframe(pd.DataFrame(ss),use_container_width=True,hide_index=True)
            st.divider()
            sec("③ ROAS 민감도")
            cg("ROAS가 N배일 때 이익. **BE ROAS 라인** 아래면 적자.")
            rr2=np.arange(max(1.,BR*0.5 if BR<900 else 1.),max(10.,RO*2),0.5)
            sn=[]
            for r in rr2:
                sp2=S*r-TC-S; nt=("← 현재" if abs(r-RO)<.26 else("← BE" if BR<900 and abs(r-BR)<.26 else ""))
                sn.append({"ROAS":f"{r:.1f}","Sales":fu(S*r),"영업이익":fu(sp2),"":("✅" if sp2>0 else "🔴")+" "+nt})
            st.dataframe(pd.DataFrame(sn),use_container_width=True,hide_index=True)
            st.divider()
            sec("④ 목표 이익 역산")
            cg("목표 영업이익 달성에 필요한 ROAS/ACoS. 현재보다 높으면 달성 어려움.")
            tc1,tc2=st.columns(2)
            with tc1: tp=st.number_input("목표($)",value=0.0,step=500.0,key="pt")
            with tc2: tb=st.number_input("예산($)",value=float(round(float(S),2)),step=500.0,key="pb")
            upo=UN/max(OR,1); cr=upo*ACU/AO if AO>0 else .7; dn=1-cr
            if dn>0 and tb>0:
                nr=(tp+tb)/(tb*dn); na=(tb/(nr*tb)*100) if nr>0 else 999
                t1,t2=st.columns(2)
                t1.metric("필요 ROAS",f"{nr:.2f}x",f"현재 {RO:.2f}x → {'✅' if RO>=nr else '❌'}")
                t2.metric("필요 ACoS",f"{na:.1f}%")
        profit_tab3()

    with tabs[3]:
        @st.fragment
        def profit_tab4():
            sec("① SKU 수익성 버블")
            cg("**축:** X=ROAS, Y=ACoS, 크기=Spend\n- 🟢 우하단 (High ROAS+Low ACoS): 스타\n"
               "- 🔴 좌상단: 문제 SKU\n- 빨간 점선 = 목표 ACoS")
            sk=_vsku()
            sgb=sk[sk["sku"].notna()].copy()
            if not sgb.empty:
                sgb["_l"]=sgb["sku"].fillna(sgb["asin"])
                cm4={"🟢 SCALE":"#10B981","🟡 HOLD":"#F59E0B","🔧 FIX":"#3B82F6","🔴 CUT":"#EF4444"}
                fb=go.Figure()
                for vd,cl in cm4.items():
                    s=sgb[sgb["verdict"]==vd]
                    if s.empty: continue
                    fb.add_trace(go.Scatter(x=s["roas"],y=s["acos_pct"],mode="markers+text",name=vd,
                        marker=dict(size=np.clip(s["spend"]/max(s["spend"].max(),1)*60+8,8,60),color=cl,opacity=.75),
                        text=s["_l"],textposition="top center",textfont=dict(size=9)))
                fb.add_hline(y=TA,line_dash="dash",line_color="#EF4444",annotation_text=f"목표 {TA:.0f}%")
                fb.update_layout(**{**BL,"height":400,"title":"SKU (ROAS vs ACoS)","xaxis_title":"ROAS","yaxis_title":"ACoS%"})
                st.plotly_chart(fb,use_container_width=True)
            st.divider()
            sec("② Tier 분류")
            cg("**T1 ⭐ 확장:** ROAS≥BE×1.2 흑자 → 예산↑\n**T2 🟡 유지:** 중간\n"
               "**T3 🔴 적자:** Pause 검토\n**T4 🚫 차단:** 주문=0 → 즉시 Pause")
            for t,d in {"T1 ⭐ 확장":"확대","T2 🟡 유지":"유지","T3 🔴 적자":"Pause 검토","T4 🚫 차단":"Pause"}.items():
                s=sk[sk["tier"]==t]
                if s.empty: continue
                with st.expander(f"{t} ({len(s)}개 · {fu(float(s['spend'].sum()))})",expanded=t.startswith("T1") or t.startswith("T3")):
                    tc2=["sku","asin","spend","sales","orders","roas","acos_pct","cpa","operating_profit"]
                    tc2=[c for c in tc2 if c in s.columns]
                    st.dataframe(s[tc2].sort_values("spend",ascending=False),use_container_width=True,hide_index=True)
            st.divider()
            if HC:
                sec("③ 구조적 적자 (SKU×Placement)")
                cg("SKU와 노출위치 조합별 BE 확인. **structural_loss=True** → 원가>매출 구조.")
                if "placement" in df.columns and df["placement"].notna().any():
                    sp4=ag(df,["sku","placement"])
                    sp4["structural_loss"]=sp4.get("be_spend",pd.Series(1))<=0
                    sl=sp4[sp4["structural_loss"]==True]
                    if sl.empty: st.success("✅ 없음")
                    else:
                        st.error(f"⛔ {len(sl)}개")
                        sc2=["sku","placement","sales","spend","orders","roas"]
                        if "operating_profit" in sl.columns: sc2.append("operating_profit")
                        sc2=[c for c in sc2 if c in sl.columns]
                        st.dataframe(sl[sc2].sort_values("spend",ascending=False),use_container_width=True,hide_index=True)
        profit_tab4()

# ───────────────── EFFICIENCY MODE ─────────────────
elif PM==MODE_EFF:

    with tabs[0]:
        @st.fragment
        def eff_tab1():
            sec("📈 주간 트렌드")
            cg("ACoS/CPC/CPA 하락(🟢)이 핵심. CTR↑+CVR↑ → 효율 개선 신호.")
            wk=df.copy(); wk["week"]=wk["date"].dt.to_period("W").apply(lambda p:p.start_time)
            wa=wk.groupby("week").agg({c:"sum" for c in _SUM_COLS if c in wk.columns}).reset_index()
            wa=rr(wa); wa=wa.sort_values("week").tail(6); wa["ws"]=wa["week"].dt.strftime("%m/%d")
            if len(wa)>=2:
                fwk=make_subplots(specs=[[{"secondary_y":True}]])
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["spend"],name="Spend",marker_color=C["spend"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["sales"],name="Sales",marker_color=C["sales"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Scatter(x=wa["ws"],y=wa["acos_pct"],name="ACoS%",mode="lines+markers",line=dict(color=C["acos"],width=2.5)),secondary_y=True)
                fwk.add_hline(y=TA,line_dash="dash",line_color="#EF4444",secondary_y=True)
                fwk.update_layout(**{**BL,"barmode":"group","height":280,"title":"주간 트렌드"})
                st.plotly_chart(fwk,use_container_width=True)
            st.divider()
            sec("📊 퍼널")
            cg("**3단계:** 노출→클릭(CTR)→주문(CVR). CTR<0.35% → 광고소재/타겟 문제. CVR<10% → 리스팅 문제.")
            f1,f2=st.columns(2)
            with f1:
                ff=go.Figure(go.Funnel(y=[f"노출 {fi(IM)}",f"클릭 {fi(CL)} CTR{fp(CT)}",f"주문 {fi(OR)} CVR{fp(CV)}"],
                    x=[IM,CL,OR],textposition="inside",textinfo="value+percent previous",marker=dict(color=["#3B82F6","#10B981","#8B5CF6"])))
                ff.update_layout(**{**BL,"height":250,"margin":dict(l=4,r=4,t=30,b=4)})
                st.plotly_chart(ff,use_container_width=True)
            with f2:
                st.markdown(f"| 지표 | 현재 | 기준 | 상태 |\n|---|---|---|---|\n"
                    f"| ACoS | {fp(AC)} | ≤{TA:.0f}% | {'✅' if AC<=TA else '🔴'} |\n"
                    f"| CTR | {fp(CT)} | ≥0.35% | {'✅' if CT>=0.35 else '🔴'} |\n"
                    f"| CVR | {fp(CV)} | ≥10% | {'✅' if CV>=10 else '🔴'} |\n"
                    f"| CPA | {fu(CA)} | ≤AOV {fu(AO)} | {'✅' if CA<=AO else '🔴'} |")
            st.divider()
            sec("캠페인 효율 (ACoS 순)")
            cg("ACoS 높은 캠페인부터 표시. 🔴 CUT → Pause 검토. ACoS>목표×2 → Bid↓ 또는 Negative 추가.")
            vc=_vcamp()
            vc2=vc.sort_values("acos_pct",ascending=False).reset_index(drop=True)
            st.plotly_chart(vtf(vc2,PM,TA,BR),use_container_width=True)
        eff_tab1()

    with tabs[1]:
        @st.fragment
        def eff_tab2():
            sec("① Negative 키워드 (주문=0, 클릭≥min)")
            cg("주문 0건인데 클릭만 발생 → Spend 낭비. **즉시 Negative 등록** 권고.\nSpend 높은 순서로 우선 처리.")
            neg=AKW[(AKW["clicks"]>=MC)&(AKW["orders"]==0)]
            if neg.empty: st.success("✅ 없음")
            else:
                st.error(f"🚨 {len(neg)}개 — 낭비 {fu(float(neg['spend'].sum()))}")
                st.dataframe(neg[["search_term","campaign","clicks","spend"]].sort_values("spend",ascending=False),
                    use_container_width=True,hide_index=True,height=300)
                st.download_button("📥 Negative CSV",neg[["search_term","campaign","clicks","spend"]].to_csv(index=False).encode("utf-8-sig"),"neg.csv",key="en")
            st.divider()
            sec("② 고비용 저성과 (ACoS > 목표×2)")
            cg("주문은 있지만 ACoS가 목표의 2배 이상. Bid↓ 또는 Pause 검토.")
            hc2=AKW[(AKW["clicks"]>=MC)&(AKW["acos_pct"]>TA*2)&(AKW["orders"]>=1)]
            if hc2.empty: st.success("✅ 없음")
            else:
                st.warning(f"⚠ {len(hc2)}개 — Spend {fu(float(hc2['spend'].sum()))}")
                hcc=["search_term","campaign","spend","sales","orders","acos_pct","roas","cpc","cvr_pct"]
                hcc=[c for c in hcc if c in hc2.columns]
                st.dataframe(hc2[hcc].sort_values("acos_pct",ascending=False),use_container_width=True,hide_index=True,height=280)
            st.divider()
            sec("③ 노출위치별 ACoS")
            cg("**3가지 위치:** 검색 상단(CTR↑ CPC↑), 검색 기타, 상품 상세(CTR↓).\n효율 좋은 위치에 집중, 비효율 위치 Bid↓.")
            if not APL.empty:
                p1,p2=st.columns(2)
                with p1: st.plotly_chart(blf(APL["placement"],{"Spend":(APL["spend"],C["spend"]),"Sales":(APL["sales"],C["sales"])},title="노출위치별",h=260),use_container_width=True)
                with p2:
                    fp3=go.Figure(); fp3.add_trace(go.Bar(x=APL["placement"],y=APL["acos_pct"],name="ACoS%",marker_color=C["acos"],text=APL["acos_pct"].map(fp),textposition="outside"))
                    fp3.add_hline(y=TA,line_dash="dash",line_color="#EF4444")
                    fp3.update_layout(**{**BL,"height":260,"title":"ACoS"}); st.plotly_chart(fp3,use_container_width=True)
        eff_tab2()

    with tabs[2]:
        @st.fragment
        def eff_tab3():
            sec("① Positive 키워드 (ACoS≤목표, 주문≥1)")
            cg("목표 이내 ACoS + 주문 발생 → Manual campaign exact match 추가 또는 Bid↑ 검토.")
            pos=AKW[(AKW["clicks"]>=MC)&(AKW["orders"]>=1)&(AKW["acos_pct"]<=TA)]
            if pos.empty: st.info("후보 없음")
            else:
                st.success(f"✅ {len(pos)}개")
                pc=["search_term","campaign","orders","clicks","spend","sales","acos_pct","roas","cvr_pct","cpa"]
                pc=[c for c in pc if c in pos.columns]
                st.dataframe(pos[pc].sort_values("orders",ascending=False).head(50),use_container_width=True,hide_index=True,height=280)
                st.download_button("📥 Positive CSV",pos[["search_term","campaign","orders","spend","acos_pct"]].to_csv(index=False).encode("utf-8-sig"),"pos.csv",key="ep")
            st.divider()
            sec("② Auto→Manual 전환")
            cg("Auto 캠페인에서 주문≥2 + ACoS≤목표 → Manual exact match로 전환. 더 정밀한 입찰 가능.")
            auto=AKW[AKW["campaign"].str.contains("Auto",case=False,na=False)]
            pr=auto[(auto["orders"]>=2)&(auto["acos_pct"]<=TA)&(auto["clicks"]>=MC)]
            if pr.empty: st.info("후보 없음")
            else:
                st.success(f"✅ {len(pr)}개")
                pc5=["search_term","campaign","orders","clicks","spend","sales","roas","acos_pct"]
                pc5=[c for c in pc5 if c in pr.columns]
                st.dataframe(pr[pc5].sort_values("orders",ascending=False),use_container_width=True,hide_index=True,height=280)
            st.divider()
            sec("③ 키워드 카테고리")
            cg("**🏷 Brand:** 자사 브랜드 → ROAS 높아야 정상\n**⚔ Competitor:** 경쟁사 → ACoS 높으면 축소\n**🔍 Generic:** 일반 키워드 → 전환율이 핵심")
            kw4=AKW[AKW["clicks"]>=MC].copy(); kw4["category"]=kw4["search_term"].apply(_ckw)
            cat4=ag(kw4,["category"])
            if not cat4.empty:
                cc=["category","spend","sales","orders","roas","acos_pct","cpc","cvr_pct"]
                cc=[c for c in cc if c in cat4.columns]; st.dataframe(cat4[cc],use_container_width=True,hide_index=True)
            st.divider()
            sec("④ 타겟 그룹별")
            cg("근접 매칭 vs 유사 매칭 vs 대체 상품 vs 보완 상품. ACoS 차이 → 타겟 유형별 Bid 조정.")
            if not ATG.empty:
                tgc=["target_group","spend","sales","orders","acos_pct","roas","cvr_pct","cpa"]
                tgc=[c for c in tgc if c in ATG.columns]
                st.dataframe(ATG[tgc].sort_values("spend",ascending=False),use_container_width=True,hide_index=True)
        eff_tab3()

    with tabs[3]:
        @st.fragment
        def eff_tab4():
            sec("① KPI 레버 (CPC/CVR/AOV)")
            cg("**각 지표를 1단위 개선하면 ROAS/이익이 얼마나 변하는지.**\n복합 적용 시 시너지 효과도 확인.")
            l1,l2,l3=st.columns(3)
            with l1: cvd=st.slider("CVR(%pt)",-3.,6.,1.,.5,key="ec")
            with l2: cpd=st.slider("CPC(%)",-40,20,-15,5,key="ep2")
            with l3: aod=st.slider("AOV(%)",-20,40,10,5,key="ea")
            def ls(ca2,pa,aa):
                nc=CV/100+ca2/100; np2=CP*(1+pa/100); na=AO*(1+aa/100)
                ns=np2*CL; no=CL*nc; nsa=no*na; nr=sd(nsa,ns)
                upo=UN/max(OR,1); nu=no*upo; nop=nsa-nu*ACU-ns; return nr,nop
            lv=[]
            for lb,ca2,pa,aa in [(f"CVR {cvd:+.1f}",cvd,0,0),(f"CPC {cpd:+d}%",0,cpd,0),(f"AOV {aod:+d}%",0,0,aod),("🔥 복합",cvd,cpd,aod)]:
                nr,nop=ls(ca2,pa,aa); lv.append({"레버":lb,"ROAS":fx(nr),"영업이익":fu(nop),"Δ":f"${nop-PR:+,.0f}"})
            st.dataframe(pd.DataFrame(lv),use_container_width=True,hide_index=True)
            st.divider()
            sec("② Bid Gap (BE CPC)")
            cg("현재 CPC vs BE CPC(손익분기 CPC). **gap>20%** → 과다입찰. CPC를 BE CPC 수준으로 인하.")
            if HC:
                bgc=["campaign","ad_group"] if "ad_group" in df.columns and df["ad_group"].notna().any() else ["campaign"]
                bg2=ag(df,bgc); bg2=bg2[bg2["clicks"]>=max(MC,5)].copy()
                if not bg2.empty and "be_spend" in bg2.columns:
                    bg2["be_cpc"]=np.where(bg2["clicks"]>0,bg2["be_spend"]/bg2["clicks"],0)
                    bg2["bid_gap"]=bg2["cpc"]-bg2["be_cpc"]
                    bg2["gap_pct"]=np.where(bg2["be_cpc"]>0,bg2["bid_gap"]/bg2["be_cpc"]*100,np.nan)
                    bg2["권고"]=bg2["gap_pct"].apply(lambda x:"⬆ 과다" if x>20 else("✅ 적정" if pd.notna(x) and abs(x)<=20 else("⬇ 여유" if pd.notna(x) and x<-20 else "⚠")))
                    ob=bg2[bg2["gap_pct"]>20]
                    b1,b2=st.columns(2)
                    kc(b1,"과다입찰",f"{len(ob)}개",f"Spend {fu(float(ob['spend'].sum()))}",color="red")
                    kc(b2,"적정",f"{len(bg2)-len(ob)}개",color="green")
                    bdc=bgc+["clicks","cpc","be_cpc","bid_gap","gap_pct","권고","spend"]
                    bdc=[c for c in bdc if c in bg2.columns]
                    st.dataframe(bg2[bdc].sort_values("bid_gap",ascending=False).head(30),use_container_width=True,hide_index=True,height=280)
            st.divider()
            sec("③ Bid 변경 시뮬")
            cg("CPC를 N% 변경 시 노출/클릭/주문 변화 예측. **가정:** Bid↑ → 노출↑(탄력성 0.5).")
            bc=st.slider("Bid(%)",-40,40,0,5,key="eb")
            if bc!=0:
                bie=0.5; bne=CP*(1+bc/100); bni=IM*(1+bc*bie/100)
                bnc=bni*(CT/100); bno=bnc*(CV/100); bns=bne*bnc; bnsa=bno*AO; bnr=sd(bnsa,bns)
                bc1,bc2,bc3=st.columns(3)
                bc1.metric("CPC",fu(bne),f"{bc:+d}%"); bc2.metric("ROAS",fx(bnr)); bc3.metric("ACoS",fp(sd(bns,bnsa,True)))
        eff_tab4()

# ───────────────── SCALE MODE ─────────────────
else:

    with tabs[0]:
        @st.fragment
        def scale_tab1():
            sec("📈 주간 트렌드")
            cg("Sales/Units 증가 추세가 핵심. ACoS가 가드레일(목표×1.5) 이내면 성장 투자 정당화.")
            wk=df.copy(); wk["week"]=wk["date"].dt.to_period("W").apply(lambda p:p.start_time)
            wa=wk.groupby("week").agg({c:"sum" for c in _SUM_COLS if c in wk.columns}).reset_index()
            wa=rr(wa); wa=wa.sort_values("week").tail(6); wa["ws"]=wa["week"].dt.strftime("%m/%d")
            if len(wa)>=2:
                fwk=make_subplots(specs=[[{"secondary_y":True}]])
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["spend"],name="Spend",marker_color=C["spend"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Bar(x=wa["ws"],y=wa["sales"],name="Sales",marker_color=C["sales"],opacity=.8),secondary_y=False)
                fwk.add_trace(go.Scatter(x=wa["ws"],y=wa["acos_pct"],name="ACoS%",mode="lines+markers",line=dict(color=C["acos"],width=2.5)),secondary_y=True)
                fwk.add_hline(y=TA,line_dash="dash",line_color="#EF4444",secondary_y=True)
                fwk.update_layout(**{**BL,"barmode":"group","height":280,"title":"주간 트렌드"})
                st.plotly_chart(fwk,use_container_width=True)
            st.divider()
            sec("📊 캠페인 (Sales 순)")
            cg("Sales 높은 캠페인부터 표시. 🟢 SCALE → ACoS 가드레일 이내 + 매출 발생. 확대 우선.")
            vc=_vcamp()
            vc2=vc.sort_values("sales",ascending=False).reset_index(drop=True)
            st.plotly_chart(vtf(vc2,PM,TA,BR),use_container_width=True)
        scale_tab1()

    with tabs[1]:
        @st.fragment
        def scale_tab2():
            sec("① 🟢 SCALE 후보")
            cg("ACoS ≤ 목표 + Sales > 0 → 확대 가능. 예산↑ 검토 대상.")
            vc=_vcamp(); sc=vc[vc["verdict"].str.contains("SCALE")]
            if sc.empty: st.info("후보 없음 — 목표 ACoS 완화 검토")
            else:
                st.success(f"✅ {len(sc)}개 — Sales {fu(float(sc['sales'].sum()))}")
                scc=["campaign","sales","orders","units","spend","acos_pct","roas","cvr_pct","cpa"]
                if "operating_profit" in sc.columns: scc.append("operating_profit")
                scc=[c for c in scc if c in sc.columns]
                st.dataframe(sc[scc].sort_values("sales",ascending=False),use_container_width=True,hide_index=True)
            st.divider()
            sec("② 캠페인 목적별 ROI")
            cg("Brand/Manual/Auto/B2B별 ROAS 비교. ROAS 높은 목적에 예산 집중.")
            cvp=vc.copy(); cvp["목적"]=cvp["campaign"].apply(_cpur)
            pg6=cvp.groupby("목적").agg(캠페인수=("campaign","count"),Spend=("spend","sum"),Sales=("sales","sum"),Orders=("orders","sum")).reset_index()
            pg6["ROAS"]=pg6["Sales"]/pg6["Spend"].replace(0,np.nan)
            fp6=make_subplots(specs=[[{"secondary_y":True}]])
            fp6.add_trace(go.Bar(x=pg6["목적"],y=pg6["Spend"],name="Spend",marker_color=C["spend"]),secondary_y=False)
            fp6.add_trace(go.Bar(x=pg6["목적"],y=pg6["Sales"],name="Sales",marker_color=C["sales"]),secondary_y=False)
            fp6.add_trace(go.Scatter(x=pg6["목적"],y=pg6["ROAS"],name="ROAS",mode="markers+text",text=pg6["ROAS"].map(fx),textposition="top center",marker=dict(size=14,color=C["roas"])),secondary_y=True)
            fp6.update_layout(**{**BL,"barmode":"group","height":310,"title":"목적별 ROI"})
            st.plotly_chart(fp6,use_container_width=True)
            st.divider()
            sec("③ T1 SKU 확장 후보")
            cg("Tier 1 = ROAS≥BE×1.2 흑자 + 주문≥2. 이 SKU의 키워드/Placement를 확대.")
            sk=_vsku(); t1=sk[sk["tier"].str.contains("T1")]
            if not t1.empty:
                st.success(f"⭐ {len(t1)}개")
                tc2=["sku","asin","sales","orders","roas","acos_pct","spend"]
                if "operating_profit" in t1.columns: tc2.append("operating_profit")
                tc2=[c for c in tc2 if c in t1.columns]
                st.dataframe(t1[tc2].sort_values("sales",ascending=False),use_container_width=True,hide_index=True)
            else: st.info("T1 없음")
        scale_tab2()

    with tabs[2]:
        @st.fragment
        def scale_tab3():
            sec("① 잠재 키워드 (확장 여지)")
            cg("주문 발생 + ACoS ≤ 목표×1.5 → Bid↑ 또는 exact match로 분리. 성장 동력.")
            pk=AKW[(AKW["orders"]>=1)&(AKW["acos_pct"]<=TA*1.5)&(AKW["clicks"]>=MC)].sort_values("orders",ascending=False)
            if pk.empty: st.info("없음")
            else:
                st.success(f"✅ {len(pk)}개")
                kcc=["search_term","campaign","orders","sales","clicks","acos_pct","roas","cvr_pct"]
                kcc=[c for c in kcc if c in pk.columns]
                st.dataframe(pk[kcc].head(50),use_container_width=True,hide_index=True,height=300)
            st.divider()
            sec("② 키워드 카테고리")
            cg("Generic 키워드 ROAS가 Brand 대비 낮지만 볼륨 크면 성장 기회. Competitor는 신중히.")
            kw4=AKW[AKW["clicks"]>=MC].copy(); kw4["category"]=kw4["search_term"].apply(_ckw)
            cat4=ag(kw4,["category"])
            if not cat4.empty:
                cc=["category","spend","sales","orders","roas","acos_pct","cpc","cvr_pct"]
                cc=[c for c in cc if c in cat4.columns]; st.dataframe(cat4[cc],use_container_width=True,hide_index=True)
                st.plotly_chart(blf(cat4["category"],{"Spend":(cat4["spend"],C["spend"]),"Sales":(cat4["sales"],C["sales"])},title="카테고리별",h=260),use_container_width=True)
            st.divider()
            sec("③ 신제품 런칭 시뮬")
            cg("가정값으로 월간 성과 예측. 기존 평균 대비 CTR/CVR 30~40% 낮게, CPC 30% 높게 시작.")
            nc1,nc2=st.columns(2)
            with nc1:
                lp=st.number_input("판매가($)",value=float(round(float(AO),2)) if AO>0 else 100.,min_value=10.,step=5.,key="slp")
                lb=st.number_input("월예산($)",value=2000.,min_value=100.,step=100.,key="slb")
                lc2=st.number_input("단위원가($)",value=float(round(float(ACU),2)) if ACU>0 else 50.,min_value=1.,step=1.,key="slc")
            with nc2:
                lt2=st.number_input("CTR(%)",value=float(max(round(CT*0.7,2),0.2)),min_value=0.1,step=0.1,key="sct")
                lcv=st.number_input("CVR(%)",value=float(max(round(CV*0.6,1),2.0)),min_value=0.5,step=0.5,key="scv")
                lcp=st.number_input("CPC($)",value=float(round(CP*1.3,2)) if CP>0 else 1.50,min_value=0.10,step=0.05,key="scp")
            ldc=lb/30; ldcl=ldc/lcp if lcp>0 else 0; ldo=ldcl*(lcv/100)
            lmo=ldo*30; lms=lmo*lp; lac=(lb/lms*100) if lms>0 else 999; lpr=lms-lmo*lc2-lb
            lr1,lr2,lr3,lr4=st.columns(4)
            lr1.metric("월 주문",f"{lmo:.0f}"); lr2.metric("월 매출",fu(lms))
            lr3.metric("ACoS",f"{lac:.1f}%"); lr4.metric("영업이익",fu(lpr))
        scale_tab3()

    with tabs[3]:
        @st.fragment
        def scale_tab4():
            sec("① 예산 증감")
            cg("ROAS 불변 가정. 확장(+50%,+100%) 시나리오 포함. 실제론 Spend↑→ROAS↓ 체감 주의.")
            ss2=[]
            for p in [-30,-20,-10,0,10,20,30,50,100]:
                ns=S*(1+p/100); nsa=ns*RO; nu=UN*(1+p/100); nop=nsa-nu*ACU-ns
                ss2.append({"변화":f"{p:+d}%","Spend":fu(ns),"Sales":fu(nsa),"영업이익":fu(nop),"":("← 현재" if p==0 else "")})
            st.dataframe(pd.DataFrame(ss2),use_container_width=True,hide_index=True)
            st.divider()
            sec("② 예산 결정 가이드")
            cg("**스코어링:** ROAS vs BE, 흑자/적자, CVR 수준 → 확대/유지/축소 판단.")
            bs=0; br=[]
            if BR<900 and RO>=BR*1.2: bs+=2; br.append("✅ ROAS>BE×1.2")
            elif BR<900 and RO>=BR: bs+=1; br.append("🟡 ROAS≥BE")
            else: bs-=2; br.append("❌ ROAS<BE")
            if PR>0: bs+=1; br.append("✅ 흑자")
            else: bs-=1; br.append("❌ 적자")
            if CV>8: bs+=1; br.append("✅ CVR 양호")
            if bs>=3: rec="📈 **예산 확대**"; rc="#10B981"
            elif bs>=1: rec="➡️ **현행 유지**"; rc="#F59E0B"
            else: rec="📉 **예산 축소**"; rc="#EF4444"
            st.markdown(f'<div style="border-left:4px solid {rc};padding:12px 16px;background:#F9FAFB;border-radius:4px">{rec}</div>',unsafe_allow_html=True)
            for x in br: st.markdown(f"  {x}")
            st.divider()
            sec("③ 6종 시나리오")
            cg("현재/Spend변동/ROAS목표/CVR개선/CPC절감 비교. 어떤 시나리오가 이익에 가장 효과적인지 한눈에.")
            if HC and UN>0:
                bp=SA-UN*ACU-S; a6=sd(SA,OR)
                sc6=[]
                sc6.append({"시나리오":"🔵 현재","Spend":fu(S),"Sales":fu(SA),"영업이익":fu(bp)})
                ns_a=S*.8; nu_a=UN*.8; p_a=ns_a*RO-nu_a*ACU-ns_a
                sc6.append({"시나리오":"🅰 -20%","Spend":fu(ns_a),"Sales":fu(ns_a*RO),"영업이익":fu(p_a)})
                ns_b=S*1.3; nu_b=UN*1.3; p_b=ns_b*RO-nu_b*ACU-ns_b
                sc6.append({"시나리오":"🅱 +30%","Spend":fu(ns_b),"Sales":fu(ns_b*RO),"영업이익":fu(p_b)})
                nc=S*5; nuc=nc/a6 if a6>0 else UN; p_c=nc-nuc*ACU-S
                sc6.append({"시나리오":"🅲 ROAS5","Spend":fu(S),"Sales":fu(nc),"영업이익":fu(p_c)})
                nud=UN*1.15; nd=nud*a6; p_d=nd-nud*ACU-S
                sc6.append({"시나리오":"🅳 CVR+15%","Spend":fu(S),"Sales":fu(nd),"영업이익":fu(p_d)})
                nse=S*.9; p_e=SA-UN*ACU-nse
                sc6.append({"시나리오":"🅴 CPC-10%","Spend":fu(nse),"Sales":fu(SA),"영업이익":fu(p_e)})
                st.dataframe(pd.DataFrame(sc6),use_container_width=True,hide_index=True)
        scale_tab4()

# ───────────────── SETTINGS TAB (all modes) ─────────────────
with tabs[4]:
    @st.fragment
    def settings_tab():
        st.header("🔧 설정 & Data Health")
        sec("① ASIN 매칭")
        cg("매칭률 90%+ → 신뢰도 높음. 70% 미만 → 원가 마스터 보완 필요.\nSpend 커버리지는 매출 기준 커버율.")
        c1,c2,c3=st.columns(3)
        c1.metric("매칭률",fp(cv["pct"]),f"{cv['mat']}/{cv['tot']}")
        c2.metric("Spend 커버",fp(cv["sp"]))
        c3.metric("행 수",fi(len(fm)))
        if cv["pct"]>=90: st.success("✅ 90%+")
        elif cv["pct"]>=70: st.warning("⚠ 70~90%")
        else: st.error("❌ <70%")
        st.divider()
        sec("② 미매칭 ASIN Top 20")
        cg("Spend 기준 상위 미매칭 ASIN. 원가 마스터에 추가하면 정확도 향상.")
        miss=fm[fm["unit_cogs"].isna()&fm["asin"].notna()&(fm["asin"]!="")]
        if not miss.empty:
            mt=miss.groupby("asin").agg(spend=("spend","sum"),sales=("sales","sum"),rows=("spend","count")).sort_values("spend",ascending=False).head(20).reset_index()
            st.dataframe(mt,use_container_width=True,hide_index=True)
        else: st.success("✅ 없음")
        st.divider()
        sec("③ 1P/3P 채널 분포")
        if "sales_channel" in df.columns:
            ch=df.groupby("sales_channel").agg(행=("spend","count"),Spend=("spend","sum"),Sales=("sales","sum")).reset_index()
            st.dataframe(ch,use_container_width=True,hide_index=True)
        st.divider()
        sec("④ BE 검증")
        cg("BE_SPEND = Sales - Σ(units×unit_cogs_adj). 양수여야 정상. 음수 → 구조적 적자.")
        be_sum=float(df["be_spend"].sum()); ok=abs(be_sum-BS)<0.01
        dbg=[{"항목":"Sales","값":fu(SA)},{"항목":"Total COGS","값":fu(TC)},
             {"항목":"BE_SPEND","값":fu(BS)},{"항목":"Σ(row be_spend)","값":fu(be_sum),"비고":"✅" if ok else "❌"},
             {"항목":"BE_ROAS","값":fx(BR) if BR<900 else "∞"},{"항목":"Avg COGS/u","값":fu(ACU)}]
        st.dataframe(pd.DataFrame(dbg),use_container_width=True,hide_index=True)
        st.divider()
        sec("⑤ Merged 샘플")
        sc2=[c for c in ["date","campaign","sales_channel","sku","asin","spend","sales","orders","units","unit_cogs","cogs_source","operating_profit"] if c in df.columns]
        st.dataframe(df[sc2].head(50),use_container_width=True,hide_index=True,height=280)
        st.download_button("📥 CSV",df.to_csv(index=False).encode("utf-8-sig"),"merged.csv",key="dm")
    settings_tab()
