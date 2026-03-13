
# Sector baskets — authoritative hardcoded list.
# This file is the sole source of truth for basket membership.
# Tickers are US-listed symbols; BRK-B is used (yfinance convention).
# Output: All text must be in English. No emoji allowed.

import logging

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────
# SECTOR BASKETS — hardcoded, edit here to change universe
# ──────────────────────────────────────────────────────────
SECTOR_BASKETS = {
    "3D PRINTING": ["PRLB", "MTLS", "SSYS", "NNDM", "DDD", "XMTR"],
    "AGRIBUSINESS": ["ELAN", "MOS", "DAR", "CF", "CTVA", "ICL", "NTR", "ADM", "BG", "DE",
                     "AGCO", "FMC", "CNH", "ZTS", "BCPC", "TTC", "TSN", "PPC"],
    "AI": ["ABSI", "ADI", "ALAB", "ALTER", "AMAT", "ANET", "ARM", "ASML", "AVGO", "BBAI",
            "CDNS", "COHR", "CRNC", "EVLV", "INOD", "KLAC", "LRCX", "MPWR", "MRVL", "MU",
            "NOW", "NVDA", "OCHR", "PATH", "ROBT", "SAP", "SMCI", "SNPS", "SOUN", "TSLA",
            "TSM", "TXN", "VERI"],
    "AIRLINES US": ["SKYW", "DAL", "UAL", "AAL", "ALK", "LUV", "SNCY", "JBLU", "ULCC"],
    "ALUMINUM": ["AA", "KALU", "CENX", "CSTM"],
    "APPAREL, FOOTWEAR & LUXURY": ["CROX", "DECK", "LULU", "NKE", "ONON", "TPR", "UAA", "VFC"],
    "ARGENTINA": ["IRS", "LOMA", "TGS", "CRESY", "CEPU", "YPF", "GGAL", "BBAR", "BMA",
                  "PAM", "TEO", "EDN", "SUPV"],
    "AUTOMOTIVE & EV": ["APTV", "F", "GM", "LEA", "RACE", "TM"],
    "BASIC MATERIALS & CHEMICALS": ["APD", "ECL", "LIN", "SHW"],
    "BATTERIES": ["AMPX", "QS", "SLDP", "ENVX", "ULBI", "SES", "MVST", "CBAT", "TE",
                  "EOSE", "TPIC"],
    "BIG BANKS & FINANCIALS": ["BAC", "BLK", "BRK-B", "BX", "C", "CB", "GS", "JPM", "MS",
                                "PGR", "SCHW", "SPGI", "WFC"],
    "BRAZIL": ["XP", "INTR", "BBD", "VIV", "TIMB", "PAGS", "ITUB", "SBS", "BSBR", "ERJ",
               "BRFS", "CIG", "ABEV", "SUZ", "EBR", "UGP", "GGB", "CSAN", "VALE", "PBR", "SID"],
    "CAR DEALERS": ["CVNA", "SAH", "PAG", "AN", "GPI", "LAD", "ABG", "ACVA", "TRUE",
                    "CARS", "KMX", "CPRT"],
    "CHEMICALS": ["ASIX", "ASPI", "BAK", "CE", "DOW", "GPRE", "HUN", "LXU", "MEOH", "OLN", "REX",
                  "RYAM", "TROX", "VHI", "WLKP", "ALB", "ALTO", "APD", "ASH", "AVNT", "AXTA", "BCPC",
                  "CBT", "CC", "CLMT", "DD", "ECL", "ECVT", "EMN", "ESI", "FUL", "GEVO", "HWKN", "IFF",
                  "IOSP", "KOP", "KRO", "KWR", "LIN", "LWLG", "LYB", "MATV", "MTX", "NEU", "NGVT",
                  "ODC", "PPG", "PRM", "RPM", "SCL", "SHW", "SOLS", "SQM", "SSL", "SXT", "WDFC", "WLK"],
    "CHINA EV": ["LI", "ZK", "XPEV", "NIO", "NIU"],
    "CHINA INTERNET": ["NTES", "TME", "BILI", "QFIN", "WB", "SOHU", "TCOM", "VIPS", "ATHM",
                        "LU", "BIDU", "BEKE", "BZ", "BABA", "PDD", "DOYU", "TAL", "KC", "JD", "IQ"],
    "COAL": ["NC", "HNRG", "ARLP", "BTU", "NRP"],
    "COAL COKING": ["AREC", "METC", "HCC", "SXC", "AMR"],
    "CONSTRUCTION & ENGINEERING": ["PWR", "EME", "CAT", "MLM", "FLR"],
    "ELECTRIFICATION": ["AIRJ", "AME", "AMPX", "AMSC", "ARRY", "ATKR", "AYI", "BDC",
                        "BLDP", "ELVA", "ENS", "ENVX", "EOSE", "ETN", "FLNC", "FPS",
                        "GEV", "GNRC", "HUBB", "MVST", "NKLR", "NNE", "NRGV", "POET",
                        "POWL", "QS", "ROK", "RRX", "SEDG", "SES", "SLDP", "SMR", "TE"],
    "CONSUMER STAPLES": ["CL", "GIS", "HSY", "KMB", "KO", "MO", "PEP", "PG", "PM"],
    "CONTRACT DRILLING": ["BORR", "HP", "NBR", "NE", "PDS", "PTEN", "RIG", "SDRL", "VAL"],
    "COPPER": ["ERO", "FCX", "HBM", "SCCO", "TECK"],
    "CRM": ["VEEV", "NICE", "BLKB", "ZEN", "CRM", "LPSN", "FRSH", "HUBS", "VRNT", "PEGA"],
    "CRUISE": ["RCL", "VIK", "CUK", "CCL", "LIND", "NCLH"],
    "CRYPTO MINING": ["IREN", "CORZ", "HUT", "CIFR", "CLSK", "RIOT", "WULF", "MARA",
                       "BTBT", "HIVE", "BTDR", "BITF", "CAN", "BMNR"],
    "CRYPTO NON MINING": ["COIN", "MSTR", "BKKT", "FIGR", "BBBY", "GEMI"],
    "CYBER SECURITY": ["ATEN", "BB", "CHKP", "CLBT", "CRWD", "CYBR", "DDOG", "FFIV",
                        "FTNT", "GEN", "NET", "OKTA", "PANW", "PLTR", "QLYS", "RPD", "S",
                        "TENB", "VRNS", "ZS"],
    "DATA CENTERS": ["TSSI", "CRDO", "IESC", "VRT", "COMM", "APLD", "FN", "ALAB", "AAOI",
                     "ALLT", "ANY", "SMCI", "ETN", "DLR", "COHR", "HPE", "SMTC", "GDS",
                     "MRVL", "LII", "RBBN", "AAON", "VNET", "SNDK", "NVTS"],
    "DEFENSE": ["LMT", "RTX", "GD", "NOC", "LHX", "BA", "HII", "TXT", "KTOS", "AVAV",
                "LDOS", "BAH", "CACI", "SAIC", "HEI", "TDG", "PLTR"],
    "DRONES": ["ONDS", "AVAV", "PRZO", "KTOS", "JOBY", "ZENA", "BLDE", "LHX", "RCAT",
               "ACHR", "GD", "ESLT", "TXT", "UMAC", "LMT", "NOC", "EH", "DPRO"],
    "ENTERPRISE IT & HARDWARE": ["ACN", "CSCO", "HPQ", "IBM", "ORCL"],
    "EUROPE": ["EWP", "EWN", "EWO", "EWU", "EWL", "EWK", "EWG", "EFNL", "EPOL", "IEUR",
               "EDEN", "IEV", "EWQ", "EWD", "EWJ", "EZU"],
    "EV": ["EVGO", "SES", "QS", "TSLA", "RIVN", "OSK", "LCID", "PT", "MGA", "ZK", "XPEV",
           "WBX", "BLNK", "NIU", "NIO", "XOS", "BYDDF", "CHPT"],
    "FERTILIZERS": ["MOS", "CF", "CTVA", "NTR", "SMG", "FMC", "SQM"],
    "FINTECH": ["ADP", "AFRM", "AX", "AXP", "CPAY", "DAVE", "DFS", "EVTC", "FI", "FIS",
                "FOUR", "GPN", "HOOD", "IMXI", "INTU", "LC", "MA", "MELI", "NU", "OPRT",
                "PAGS", "PAYO", "PYPL", "QTWO", "SEZL", "SOFI", "SQ", "STNE", "TOST",
                "TREE", "UPST", "V", "VIRT", "WEX", "WU", "XYZ"],
    "FUEL CELL": ["QS", "PLUG", "BLDP", "SES", "BE", "HYDR", "FCEL"],
    "GAMBLING": ["SGHC", "RSI", "MLCO", "RRR", "BYD", "FLUT", "DKNG", "WYNN", "LVS",
                 "MCRI", "GDEN", "MGM", "CZR", "PENN"],
    "GAMBLING ONLINE": ["SGHC", "RSI", "FLUT", "DKNG"],
    "GOLD": ["AEM", "AU", "BTG", "EGO", "EQX", "GFI", "GOLD", "HMY", "IAG", "KGC",
             "NEM", "NGD", "OR", "ORLA", "RGLD"],
    "HEALTHCARE": ["ABBV", "ABSI", "ABT", "AG", "ALLO", "ALNY", "ALT", "AMGN", "AMWL",
                   "ARWR", "BEAM", "BMRN", "BMY", "BNTX", "BSX", "CDNA", "CERT", "CRBU",
                   "CRSP", "CVS", "DHR", "DOCS", "DTIL", "EDIT", "ELV", "FLGT", "GDRX",
                   "GPCR", "GRAL", "HIMS", "HINGE", "ILMN", "ISRG", "JNJ", "LD", "LEGN",
                   "LFMD", "LLY", "MRK", "MRNA", "MYGN", "NTLA", "NTRA", "NVAX", "NVO",
                   "OZEM", "PACB", "PFE", "QGEN", "QURE", "RARE", "REGN", "RGNX", "RNA",
                   "RXRX", "SANA", "SDGR", "SGMO", "SLP", "SRPT", "SRRK", "SYK", "TDOC",
                   "TECH", "TEM", "THNR", "TMO", "TXG", "UNH", "VCYT", "VERV", "VIR",
                   "VKTX", "VRTX", "VTRS"],
    "HOMEBUILDING": ["JCI", "TT", "ALLE", "CSL", "CARR", "GRBK", "WMS", "BLD", "BZH",
                     "TOL", "HD", "DFH", "IBP", "TMHC", "PHM", "DHI", "NVR", "AOS", "TPH",
                     "LII", "LOW", "WSM", "LEN", "HOV", "MHO", "FND", "OC", "MTH", "MAS",
                     "BLDR", "KBH", "CCS", "CVCO", "LGIH", "SKY"],
    "INDUSTRIAL DISTRIBUTION": ["FERG", "GIC", "CNM", "REZI", "TRNS", "WCC", "EVI", "TITN",
                                  "MSM", "GWW", "AIT", "SITE", "DXPE", "DSGR", "POOL", "BXC",
                                  "DNOW", "WSO", "FAST"],
    "INDUSTRIALS & CONGLOMERATES": ["EMR", "GE", "HON", "MMM", "ROK"],
    "INSURANCE BROKERS": ["TWFG", "AJG", "CRVL", "MMC", "BWN", "AON", "BRO", "WTW",
                           "ERIE", "GSHD", "SLQT"],
    "INTERNET B2C": ["SPOT", "DASH", "META", "ROOT", "FVRR", "AMZN", "PINS", "CPNG",
                     "GOOGL", "MTCH", "YELP", "SNAP", "IAC"],
    "LIDAR": ["AEVA", "OUST", "INVZ", "LIDR", "MBLY", "HSAI", "LAZR"],
    "LIQUEFIED NATURAL GAS": ["CQP", "E", "EE", "FLNG", "GLNG", "KMI", "LNG", "NEXT",
                               "OKE", "SRE", "VG", "WMB"],
    "LITHIUM": ["SLI", "LIT", "LAC", "PLL", "SQM", "ALB", "ATLX"],
    "LOGISTICS": ["PBI", "FWRD", "CHRW", "EXPD", "HUBG", "FDX", "UPS", "ZTO", "JBHT"],
    "MAG7": ["NVDA", "MSFT", "TSLA", "META", "AMZN", "GOOG", "AAPL"],
    "MARINE SHIPPING": ["HAFN", "KEX", "SBLK", "BWLP", "ZIM", "GOGL", "MATX", "TDW"],
    "MEDIA & ENTERTAINMENT": ["DIS", "EA", "FOXA", "LYV", "NFLX", "PARA", "TTWO", "WBD"],
    "OIL AND GAS DOWNSTREAM": ["PARR", "CVI", "DK", "DINO", "MPC", "PBF", "VLO", "PSX",
                                 "CSAN", "IEP"],
    "OIL AND GAS EQUIPMENT AND SERVICES": ["FTI", "MRC", "VAL", "TDW", "VTOL", "ACDC",
                                             "OII", "WHD", "AROC", "TS", "WFRD", "KGS", "USAC",
                                             "INVX", "RES", "BKR", "NOV", "CHX", "XPRO", "HAL",
                                             "WTTR", "SLB", "LBRT", "CLB", "HLX", "AESI"],
    "OIL AND GAS MIDSTREAM": ["INSW", "FRO", "DTM", "STNG", "AM", "GEL", "LNG", "GLNG",
                               "ENB", "WMB", "KMI", "MPLX", "TRMD", "DKL", "TRP", "ET",
                               "EPD", "PBA", "WES", "PAA", "PAGP", "HESM", "CQP", "VNOM",
                               "TRGP", "KNTK", "OKE"],
    "OIL AND GAS UPSTREAM": ["APA", "AR", "BP", "CHRD", "CIVI", "CNX", "COP", "CRK",
                              "CTRA", "CVI", "CVX", "DINO", "DK", "DVN", "EOG", "EQT",
                              "FANG", "HES", "KOS", "MGY", "MPC", "MTDR", "MUR", "NOG",
                              "OVV", "OXY", "PBF", "PR", "PSX", "RRC", "SHEL", "SM",
                              "TPL", "TTE", "VLO", "VTLE", "XOM"],
    "OPTICAL NETWORKING": ["AXTI", "AAOI", "LITE", "GLW", "IPGP", "LASR", "VIAV",
                            "CIEN", "COHR", "TTMI", "ON", "PLAB", "PPG", "CSCO", "SHW",
                            "LRCX", "ANET"],
    "PAYMENT PROCESSING": ["STNE", "AFRM", "PAGS", "TOST", "FOUR", "AXP", "QTWO", "FIS",
                            "XYZ", "PYPL", "DFS", "MA", "V", "EVTC", "WEX", "CPAY", "PAYO",
                            "WU", "GPN", "FI", "IMXI", "SEZL"],
    "QUANTUM COMPUTING": ["ARQQ", "QUBT", "QRTS", "IONQ", "RGTI", "QTUM", "QMCO"],
    "RAILROAD": ["RAIL", "CSX", "NSC", "CP", "WAB", "CNI", "FSTR", "UNP", "TRN",
                 "GBX", "RVSN"],
    "RARE EARTH": ["TMC", "USAR", "MP", "PPTA", "NB", "CRML", "UAMY", "METC"],
    "REAL ESTATE & REITs": ["AMT", "CCI", "EQIX", "O", "PLD", "PSA", "SPG", "VTR", "WELL"],
    "REGIONAL BANK": ["BPOP", "AX", "CUBI", "ABCB", "HBAN", "EWBC", "FBP", "WTFC", "MTB",
                      "HWC", "ASB", "SNV", "FHN", "UBS", "OZK", "FNB", "RF", "CFG", "CATY",
                      "CVBF", "CFR", "WBS", "CADE", "TCBI", "HOMB", "TFC", "FFIN", "ZION",
                      "TBBK", "ONB", "UMBF", "BKU", "FULT", "CBSH", "FHB", "PNFP", "PFS",
                      "VLY", "FIBK", "PB", "BOH", "BANC", "INDB", "GBCI", "SSB", "WAL",
                      "BOKF", "COLB"],
    "RESTAURANTS": ["PC", "EAT", "DIN", "KRUS", "JACK", "PLAY", "WING", "BJRI", "CAKE",
                    "RAVE", "CMG", "ARCO", "CAVA", "YUMC", "SBUX", "DRI", "TXRH", "YUM",
                    "BH", "PTLO", "MCD", "FWRG", "QSR", "LOCO", "CBRL", "SG", "SHAK",
                    "BROS", "DPZ", "WEN", "BLMN", "NATH", "NDLS", "STKS", "CNNE", "CHA",
                    "PZZA", "BRCB", "HCHL"],
    "RETAIL & DEPARTMENT STORES": ["BURL", "COST", "DDS", "DG", "DLTR", "HD", "KSS",
                                    "LOW", "M", "ROST", "TGT", "TJX", "WMT"],
    "ROBOTICS": ["ADSK", "AI", "AMBA", "APPN", "ARBE", "AVAV", "AZTA", "CDNS", "CGNX",
                 "CRNC", "DE", "DT", "EMR", "FARO", "GMED", "GXO", "HLX", "HSAI", "ILMN",
                 "IOT", "IPGP", "ISRG", "ISRO", "JOBY", "JRBT", "KRTI", "MANH", "NDSN",
                 "NOVT", "NVDA", "OMCL", "PATH", "PEGA", "PR", "PRCT", "PRO", "PTC",
                 "QCOM", "ROK", "SERV", "SOUN", "SYK", "SYM", "TER", "TRMB", "TSLA",
                 "UPST", "ZBRA"],
    "SEMICONDUCTORS": ["AVGO", "MCHP", "MU", "NVDA", "AMD", "STM", "KLAC", "LRCX", "TSM",
                        "AMAT", "ON", "SMCI", "MPWR", "ASML", "TXN", "CDNS", "ADI", "QRVO",
                        "SWKS", "SNPS", "MRVL", "OLED", "NXPI", "TER", "QCOM", "INTC"],
    "SEMIS NON AI": ["STM", "ON", "MCHP", "ADI", "TXN", "MPWR", "NXPI", "OLED", "QRVO",
                     "SWKS", "QCOM"],
    "SILVER": ["AG", "CDE", "EXK", "FNV", "MAG", "PAAS", "SVM", "WPM"],
    "SOFTWARE": ["ADBE", "ASAN", "BILL", "BL", "CFLT", "CLBT", "CRM", "CRWD", "CYBR",
                 "DDOG", "DOCN", "ESTC", "FROG", "FRSH", "GTLB", "HUBS", "IGV", "KVYO",
                 "MDB", "MGNI", "NET", "NOW", "NTNX", "PATH", "PEGA", "PL", "PLTR",
                 "RBRK", "SE", "SHOP", "SNOW", "TEAM", "TWLO", "WIX", "ZM", "ZS"],
    "SOLAR": ["SHLS", "NXT", "FSLR", "RUN", "CSIQ", "ARRY", "SEDG", "RNW", "CWEN",
              "JKS", "HASI", "DQ", "ENPH", "NOVA"],
    "SPACE": ["BKSY", "AVAV", "ASTS", "RKLB", "RDW", "PL", "KRMN", "ARKX", "LUNR",
              "HON", "IRDM", "SATS", "SATL", "SPCE", "SIDU"],
    "STARGATE": ["ORCL", "NVDA", "ARM", "MSFT", "MRVL", "HIMX"],
    "STEEL": ["MT", "WS", "ZEUS", "RIO", "VALE", "CMC", "NUE", "STLD", "MTUS", "ASTL",
              "RS", "NWPX", "TX", "PKX", "SID", "CLF", "GGB", "HCC", "MSB"],
    "TRAVEL & LEISURE": ["ABNB", "BKNG", "EXPE", "HLT", "LYFT", "MAR", "UBER"],
    "URANIUM AND NUCLEAR": ["LEU", "SMR", "OKLO", "CCJ", "ASPI", "GEV", "VST", "CEG",
                             "MIR", "BWXT", "UUUU", "NXE", "URG", "UROY", "UEC", "DNN",
                             "NNE", "AMTM"],
    "UTILITIES & TELECOM": ["CMCSA", "DUK", "NEE", "SO", "T", "TMUS", "VZ"],
}

# Log summary at import
_n_baskets = len(SECTOR_BASKETS)
_n_tickers = sum(len(v) for v in SECTOR_BASKETS.values())
_unique = len({t for tickers in SECTOR_BASKETS.values() for t in tickers})
logger.info(f"Baskets loaded: {_n_baskets} baskets, {_n_tickers} entries, {_unique} unique tickers")


def get_baskets():
    """Return the sector baskets dictionary."""
    return SECTOR_BASKETS


def get_all_basket_tickers():
    """
    Return a deduplicated list of all tickers across all baskets.
    This is the scanning universe — every ticker the screener will process.
    """
    seen = set()
    tickers = []
    for basket_tickers in SECTOR_BASKETS.values():
        for t in basket_tickers:
            if t not in seen:
                seen.add(t)
                tickers.append(t)
    return tickers


def build_ticker_basket_map():
    """
    Build a flat mapping: ticker -> basket name.
    When a ticker appears in multiple baskets the first basket listed wins,
    preserving the most specific/primary classification.
    """
    mapping = {}
    for basket, tickers in SECTOR_BASKETS.items():
        for t in tickers:
            if t not in mapping:          # first-basket-wins
                mapping[t] = basket
    return mapping


def get_deep_sector(ticker, basket_map):
    """
    Return (sector_label, source) for a ticker using the basket map.
    Tickers not in any basket are labelled 'Other'.
    No external API calls are made.
    """
    if ticker in basket_map:
        return (basket_map[ticker], 'basket')
    return ("Other", 'unmapped')


def analyze_baskets(full_df):
    """
    Analyse sector basket performance from the scored DataFrame.
    Only baskets that have at least one ticker in full_df are included.
    Returns a DataFrame sorted by Avg Score descending.
    """
    import pandas as pd

    basket_stats = []

    for basket, tickers in SECTOR_BASKETS.items():
        mask = full_df.index.isin(tickers)
        basket_df = full_df[mask]

        if basket_df.empty:
            continue

        avg_score = basket_df['Final_Score'].mean()
        avg_3m = basket_df['3m_return'].mean() if '3m_return' in basket_df.columns else 0.0
        avg_1m = basket_df['1m_return'].mean() if '1m_return' in basket_df.columns else 0.0
        avg_1w = basket_df['1w_return'].mean() if '1w_return' in basket_df.columns else 0.0
        avg_3d = basket_df['3d_return'].mean() if '3d_return' in basket_df.columns else 0.0

        top_stock = basket_df.sort_values(by='Final_Score', ascending=False).index[0]

        basket_stats.append({
            "Basket": basket,
            "Avg Score": round(avg_score, 2),
            "3M %": round(avg_3m * 100, 2),
            "1M %": round(avg_1m * 100, 2),
            "1W %": round(avg_1w * 100, 2),
            "3D %": round(avg_3d * 100, 2),
            "Count": len(basket_df),
            "Top Pick": top_stock,
        })

    if not basket_stats:
        return pd.DataFrame()

    return pd.DataFrame(basket_stats).sort_values(by="Avg Score", ascending=False)


def get_basket_top_stocks(full_df, top_n=5):
    """
    For each basket, return the top N stocks by Final_Score
    found in the current scan. Returns dict: {basket_name: DataFrame}
    """
    result = {}
    for basket, tickers in SECTOR_BASKETS.items():
        basket_stocks = full_df[full_df.index.isin(tickers)]
        if basket_stocks.empty:
            continue
        result[basket] = basket_stocks.sort_values('Final_Score', ascending=False).head(top_n)
    return result


def analyze_baskets_short(full_df):
    """
    Analyse sector basket weakness from the scored DataFrame.
    Uses Short_Score if available, otherwise inverts Final_Score.
    Returns a DataFrame sorted by Avg Short Score descending (weakest first).
    """
    import pandas as pd

    score_col = 'Short_Score' if 'Short_Score' in full_df.columns else 'Final_Score'
    basket_stats = []

    for basket, tickers in SECTOR_BASKETS.items():
        mask = full_df.index.isin(tickers)
        basket_df = full_df[mask]

        if basket_df.empty:
            continue

        avg_score = basket_df[score_col].mean()
        avg_3m = basket_df['3m_return'].mean() if '3m_return' in basket_df.columns else 0.0
        avg_1m = basket_df['1m_return'].mean() if '1m_return' in basket_df.columns else 0.0
        avg_1w = basket_df['1w_return'].mean() if '1w_return' in basket_df.columns else 0.0
        avg_3d = basket_df['3d_return'].mean() if '3d_return' in basket_df.columns else 0.0

        if score_col == 'Short_Score':
            worst_stock = basket_df.sort_values(by=score_col, ascending=False).index[0]
        else:
            worst_stock = basket_df.sort_values(by='Final_Score', ascending=True).index[0]

        basket_stats.append({
            "Basket": basket,
            "Avg Score": round(avg_score, 2),
            "3M %": round(avg_3m * 100, 2),
            "1M %": round(avg_1m * 100, 2),
            "1W %": round(avg_1w * 100, 2),
            "3D %": round(avg_3d * 100, 2),
            "Count": len(basket_df),
            "Worst Pick": worst_stock,
        })

    if not basket_stats:
        return pd.DataFrame()

    stats_df = pd.DataFrame(basket_stats)
    if score_col == 'Short_Score':
        return stats_df.sort_values(by="Avg Score", ascending=False)
    else:
        return stats_df.sort_values(by="Avg Score", ascending=True)


def get_basket_bottom_stocks(full_df, top_n=5):
    """
    For each basket, return the worst N stocks (best short candidates).
    Uses Short_Score if available, otherwise sorts Final_Score ascending.
    Returns dict: {basket_name: DataFrame}
    """
    result = {}
    score_col = 'Short_Score' if 'Short_Score' in full_df.columns else 'Final_Score'

    for basket, tickers in SECTOR_BASKETS.items():
        basket_stocks = full_df[full_df.index.isin(tickers)]
        if basket_stocks.empty:
            continue
        if score_col == 'Short_Score':
            result[basket] = basket_stocks.sort_values(score_col, ascending=False).head(top_n)
        else:
            result[basket] = basket_stocks.sort_values('Final_Score', ascending=True).head(top_n)

    return result
