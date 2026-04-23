
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
    "3D PRINTING": ["XMTR", "PRLB", "SSYS", "NNDM", "MTLS", "DDD"],
    "AGRIBUSINESS": ["FMC", "BCPC", "TSN", "AGCO", "CNH", "ZTS", "DE", "DAR", "CTVA", "ICL", "TTC", "BG", "ELAN", "ADM", "PPC", "NTR", "CF", "MOS"],
    "AI": ["LWLG", "MRVL", "MPWR", "ALAB", "NVTS", "COHR", "NBIS", "ARM", "AVGO", "LITE", "ABSI", "TER", "EVLV", "CRNC", "SNDK", "HIMX", "TXN", "KLAC", "ADI", "LRCX", "POET", "AMAT", "NVDA", "TSM", "SOLS", "SOUN", "ASML", "AI", "CDNS", "ROBT", "INOD", "SNPS", "TSLA", "BBAI", "SAP", "MU", "SMCI", "NOW", "PATH", "VERI"],
    "AI DRUG DEVELOPMENT": ["ABSI", "SLP", "RXRX", "SDGR", "CERT"],
    "AIRLINES US": ["ULCC", "JBLU", "ALK", "AAL", "SNCY", "DAL", "SKYW", "UAL", "LUV"],
    "ALUMINUM": ["KALU", "CSTM", "CENX", "AA"],
    "ARGENTINA": ["BBAR", "BMA", "SUPV", "GGAL", "IRS", "LOMA", "TEO", "YPF", "CRESY", "EDN", "CEPU", "TGS", "PAM"],
    "ARKK": ["IRDM", "TXG", "AMD", "TWST", "PACB", "TER", "BEAM", "XYZ", "CRSP", "HOOD", "ROKU", "AMZN", "CERS", "TEM", "RXRX", "NTLA", "SOFI", "BWXT", "META", "NVDA", "ILMN", "TSM", "ABNB", "PINS", "NTRA", "SHOP", "GH", "DE", "RBLX", "VCYT", "TSLA", "ACHR", "COIN", "PLTR", "GTLB", "TTD", "PD", "DKNG", "CRCL"],
    "BATTERIES": ["ENVX", "EOSE", "ULBI", "MVST", "SLDP", "SES", "QS", "AMPX", "FLNC", "CBAT", "TE"],
    "BRAZIL": ["GGB", "VALE", "SBS", "BBD", "CIG", "UGP", "SID", "PAGS", "ITUB", "XP", "BSBR", "MELI", "ABEV", "TIMB", "VIV", "INTR", "CSAN", "PBR", "SUZ"],
    "CANNABIS": ["CGC", "VFF", "SNDL", "ACB", "GRWG", "MSOS", "OGI", "CRON", "TLRY", "HITI", "IIPR"],
    "CAR DEALERS": ["CVNA", "CARS", "SAH", "GPI", "LAD", "PAG", "AN", "ABG", "ACVA", "CPRT", "KMX"],
    "CHEMICALS": ["LWLG", "PRM", "FUL", "ESI", "ODC", "CC", "ALB", "TROX", "SQM", "KRO", "SXT", "KWR", "AVNT", "ASH", "ECVT", "ASPI", "PPG", "SCL", "NGVT", "IOSP", "MATV", "RPM", "IFF", "SHW", "HUN", "CLMT", "AXTA", "HWKN", "CBT", "BCPC", "SOLS", "MTX", "DD", "ALTO", "EMN", "VHI", "ECL", "REX", "CE", "MEOH", "APD", "ASIX", "NEU", "LIN", "SSL", "KOP", "WLK", "WLKP", "OLN", "WDFC", "DOW", "LXU", "GPRE", "RYAM", "LYB", "BAK", "GEVO"],
    "CHINA": ["KC", "NIO", "IQ", "TAL", "JD", "HTHT", "YMM", "TUYA", "LI", "PONY", "QFIN", "EDU", "BABA", "PDD", "BIDU", "ATAT", "BZ", "EH", "WB", "VIPS", "GDS", "VNET", "DQ", "BILI", "ZTO", "BEKE", "TME", "XPEV", "CHA", "HSAI", "RLX", "YUMC", "LU", "FINV"],
    "CHINA EV": ["NIO", "LI", "NIU", "XPEV"],
    "COAL": ["NC", "NRP", "HNRG", "ARLP", "BTU"],
    "COAL COKING": ["SXC", "METC", "HCC", "AMR", "AREC"],
    "COMMUNICATION EQUIPMENT": ["AAOI", "UI", "GSAT", "VIAV", "CIEN", "VSAT", "LITE", "HPE", "NOK", "BDC", "ZBRA", "GILT", "CSCO", "DGII", "VISN", "ERIC", "MSI", "ASTS", "ONDS"],
    "COPPER": ["SCCO", "NEXA", "AUGO", "TECK", "HBM", "ERO", "FCX"],
    "CRM": ["RNG", "FRSH", "PEGA", "CRM", "VEEV", "NICE", "BLKB", "HUBS"],
    "CRUISE": ["CUK", "CCL", "VIK", "LIND", "RCL", "NCLH"],
    "CRYPTO MINING": ["HUT", "BTDR", "WULF", "CIFR", "RIOT", "MARA", "CAN", "CORZ", "CLSK", "HIVE", "IREN", "BMNR", "BTBT", "BITF"],
    "CRYPTO NON MINING": ["BBBY", "MSTR", "FIGR", "BKKT", "COIN", "GEMI", "CRCL"],
    "CYBER SECURITY": ["BB", "ATEN", "FFIV", "PANW", "VRNS", "FTNT", "CRWD", "S", "PLTR", "DDOG", "TENB", "OKTA", "RPD", "CLBT", "NET", "CHKP", "ZS", "QLYS", "QLYS"],
    "DATA CENTERS": ["LWLG", "AAOI", "MRVL", "CRDO", "SMTC", "CLS", "CRWV", "FN", "TSSI", "ALAB", "NVTS", "COHR", "NBIS", "DELL", "RBBN", "LITE", "GLW", "HPE", "IESC", "SNDK", "MOD", "HIMX", "AAON", "KEYS", "APLD", "POET", "VRT", "DLR", "ETN", "ALLT", "LII", "GDS", "VNET", "SMCI"],
    "DATA STORAGE": ["STX", "WDC", "SNDK", "PSTG", "NTAP", "MU"],
    "DEPARTMENT STORES": ["KSS", "BURL", "M", "ROST", "TJX", "DDS"],
    "DRONES": ["RDW", "EH", "TXT", "ACHR", "ZENA", "JOBY", "LHX", "GD", "LMT", "NOC", "ONDS", "ESLT", "DPRO", "AVAV", "KTOS", "RCAT", "UMAC", "PRZO"],
    "EDGE CLOUD": ["NBIS", "DELL", "LUMN", "HPE", "ANET", "EQIX", "FFIV", "DDOG", "FSLY", "NET", "AKAM", "ZS"],
    "ELECTRIFICATION": ["AEHR", "NKLR", "ON", "ENVX", "EOSE", "AMSC", "ELVA", "ATKR", "NNE", "ENS", "MOD", "ROK", "GEV", "POET", "BDC", "BLDP", "ARRY", "ETN", "MVST", "AME", "HUBB", "NVT", "RRX", "AYI", "SLDP", "SES", "GNRC", "SMR", "QS", "AEP", "AIRJ", "NEE", "NRGV", "AMPX", "SRE", "SO", "FPS", "DUK", "SEI", "CEG", "SEDG", "FLNC", "TE", "POWL"],
    "ENERGY SHIPPING": ["TRMD", "FRO", "TNK", "NAT", "STNG", "LPG", "INSW", "TEN", "ASC", "NVGS", "ECO", "DHT", "FLNG"],
    "ENGINEERING & CONSTRUCTION": ["LGN", "AGX", "MYRG", "MTZ", "IESC", "PRIM", "TPC", "BLD", "FIX", "DY", "APG", "FER", "ECG", "ACA", "STRL", "EME", "ROAD", "FLR", "CDLR", "PWR", "EXPO", "WLDN", "GVA", "STN", "TTEK", "J", "KBR", "ACM"],
    "EUROPE": ["EPOL", "EWO", "EWI", "EWK", "EWD", "EDEN", "EWN", "EWP", "IEUR", "IEV", "EWG", "EIRL", "EWQ", "EWL", "EFNL", "EWU", "ENOR"],
    "EV": ["AEHR", "BLNK", "NIO", "EVGO", "MGA", "RIVN", "LI", "SES", "NIU", "QS", "TSLA", "OSK", "XPEV", "XOS", "ZK"],
    "FERTILIZERS": ["SQM", "FMC", "SMG", "CTVA", "NTR", "CF", "MOS"],
    "FINTECH": ["AFRM", "OPRT", "UPST", "LC", "CHYM", "VIRT", "SEZL", "DAVE", "XYZ", "HOOD", "AX", "CPAY", "WEX", "PAGS", "TREE", "PAYO", "XP", "PYPL", "AXP", "SOFI", "FOUR", "NU", "STNE", "MA", "EVTC", "V", "WU", "QTWO", "TOST", "GPN", "FIS", "ADP", "INTU"],
    "FUEL CELL": ["BE", "PLUG", "HYDR", "BLDP", "FCEL"],
    "GAMBLING": ["PENN", "RSI", "MLCO", "WYNN", "LVS", "GDEN", "BYD", "SGHC", "MGM", "MCRI", "FLUT", "CZR", "RRR", "DKNG"],
    "GAMBLING ONLINE": ["RSI", "SGHC", "FLUT", "DKNG"],
    "GENOMICS": ["TXG", "EDIT", "PACB", "SRPT", "BEAM", "ARWR", "CRSP", "CDNA", "RARE", "QURE", "LEGN", "TECH", "CRBU", "VIR", "RGNX", "BNTX", "TEM", "DTIL", "SANA", "NTLA", "ALLO", "ILMN", "A", "MYGN", "FLGT", "NTRA", "GRAL", "MRNA", "VCYT", "QGEN", "REGN", "BMRN", "GILD", "VRTX", "ALNY", "SGMO"],
    "GLP1": ["SRRK", "NVO", "VKTX", "ALT", "OZEM", "AMGN", "THNR", "LLY", "OMDA", "GPCR", "HNGE", "WVE"],
    "GOVERNMENT CONTRACTORS": ["PSN", "TTEK", "MMS", "TYL", "SAIC", "PLTR", "LHX", "GD", "LDOS", "HII", "LMT", "NOC", "CACI"],
    "HEALTH INSURANCE": ["HUM", "ALHC", "OSCR", "UNH", "ELV", "CVS", "CNC", "CI", "MOH"],
    "HOMEBUILDING": ["OC", "BLD", "WMS", "LGIH", "MTH", "CCS", "TT", "MAS", "CVCO", "BZH", "HOV", "DHI", "LOW", "GRBK", "WSM", "NVR", "PHM", "TOL", "SKY", "IBP", "CSL", "CARR", "JCI", "HD", "TMHC", "DFH", "KBH", "MHO", "BLDR", "LII", "AOS", "TPH", "ALLE", "FND", "LEN"],
    "INDEX QQQE": ["MRVL", "INTC", "AMD", "ON", "ARM", "AVGO", "GFS", "MCHP", "TXN", "KLAC", "AMZN", "ODFL", "ADI", "LRCX", "MAR", "CDW", "CHTR", "PYPL", "AMAT", "META", "DASH", "NXPI", "NVDA", "GOOGL", "GOOG", "CSCO", "PCAR", "ROST", "ILMN", "MELI", "CSX", "AZN", "AAPL", "ABNB", "MSFT", "SBUX", "ASML", "ORLY", "CDNS", "GEHC", "BKR", "SNPS", "PDD", "TTWO", "QCOM", "MNST", "CMCSA", "ROP", "CCEP", "PEP", "NFLX", "IDXX", "MRNA", "AEP", "TSLA", "KHC", "CTSH", "FAST", "COST", "EA", "MDLZ", "HON", "XEL", "AMGN", "LULU", "PAYX", "LIN", "CPRT", "REGN", "WBD", "PANW", "ADBE", "DLTR", "ISRG", "KDP", "GILD", "MDB", "FTNT", "VRTX", "MU", "ADSK", "BIIB", "CRWD", "CTAS", "DDOG", "ADP", "TMUS", "EXC", "FANG", "DXCM", "TTD", "WDAY", "SMCI", "CSGP", "CEG", "TEAM", "INTU", "VRSK", "ZS", "BKNG", "ANSS"],
    "INDUSTRIAL DISTRIBUTION": ["TITN", "WCC", "DXPE", "BXC", "REZI", "FERG", "WSO", "AIT", "SITE", "GIC", "GWW", "MSM", "POOL", "TRNS", "CNM", "DSGR", "EVI", "DNOW", "FAST"],
    "INSURANCE BROKERS": ["SLQT", "BWIN", "AJG", "AON", "WTW", "CRVL", "BRO", "ERIE", "TWFG", "GSHD"],
    "INT FREIGHT AND LOGISTICS": ["FWRD", "PBI", "HUBG", "JBHT", "FDX", "UPS", "CHRW", "EXPD", "ZTO"],
    "INTERNET B2C": ["SNAP", "NEGG", "AMZN", "OPRA", "IAC", "MTCH", "YELP", "RDDT", "META", "DASH", "GOOGL", "PINS", "SPOT", "FVRR", "FIG"],
    "LIDAR": ["OUST", "AEVA", "MBLY", "HSAI", "INVZ", "LIDR"],
    "LITHIUM": ["ALB", "LIT", "SQM", "LAC", "ATLX", "SLI"],
    "LNG": ["GLNG", "FLNG", "EE", "SRE", "WMB", "KMI", "E", "OKE", "NEXT", "LNG", "CQP", "VG"],
    "MAG7": ["AMZN", "META", "NVDA", "GOOG", "AAPL", "MSFT", "TSLA"],
    "MARINE SHIPPING": ["BWLP", "HAFN", "TDW", "MATX", "KEX", "SBLK", "ZIM"],
    "MEME": ["AMC", "GPRO", "CRML", "NVTS", "QUBT", "QBTS", "CIFR", "FLWS", "CLSK", "BYND", "APLD", "OKLO", "DXYZ", "RXRX", "IREN", "SOUN", "USAR", "GME", "QS", "BBAI", "DNUT", "OPEN", "LEU", "DVLT"],
    "MINERS": ["HMY", "SVM", "SSRM", "HYMC", "KGC", "AU", "WPM", "AGI", "GFI", "EQX", "OR", "PAAS", "ORLA", "AEM", "RGLD", "NEM", "BTG", "FNV", "EXK", "IAG", "AG", "CDE", "EGO", "NGD"],
    "NVIDIA VC": ["MRVL", "NBIS", "WRD", "ARM", "NOK", "HIMX", "APLD", "RXRX", "NVDA"],
    "OIL AND GAS DOWNSTREAM": ["CSAN", "PARR", "IEP", "DINO", "VLO", "MPC", "CVI", "PSX", "DK", "PBF"],
    "OIL AND GAS DRILLING": ["BORR", "SDRL", "NBR", "NE", "VAL", "RIG", "HP", "PTEN", "PDS"],
    "OIL AND GAS EQUIPMENT AND SERVICES": ["WFRD", "SLB", "WHD", "TDW", "KGS", "VTOL", "OII", "FTI", "TS", "NOV", "BKR", "INVX", "CLB", "HAL", "AROC", "WTTR", "XPRO", "HLX", "AESI", "SEI", "VAL", "RES", "USAC", "ACDC", "LBRT"],
    "OIL AND GAS MIDSTREAM": ["TRMD", "FRO", "STNG", "INSW", "GLNG", "TRGP", "KNTK", "ET", "DTM", "ENB", "WES", "WES", "EPD", "WMB", "KMI", "PAGP", "OKE", "PAA", "PBA", "MPLX", "VNOM", "GEL", "HESM", "TRP", "LNG", "CQP", "DKL", "AM"],
    "OIL AND GAS UPSTREAM": ["CVE", "PR", "APA", "MUR", "RRC", "MTDR", "SM", "DINO", "FANG", "CHRD", "OVV", "VLO", "CNX", "COP", "EOG", "KOS", "XOM", "CVX", "EQT", "CTRA", "OXY", "DVN", "MGY", "MPC", "CVI", "PSX", "NOG", "AR", "CRK", "DK", "TPL", "PBF"],
    "OPTICAL NETWORK INFRA": ["LWLG", "AAOI", "AXTI", "FN", "ON", "PLAB", "VIAV", "COHR", "TTMI", "CIEN", "LITE", "GLW", "ANET", "LRCX", "PPG", "SHW", "CSCO", "IPGP", "LASR"],
    "PAYMENT PROCESSING": ["AFRM", "SEZL", "XYZ", "CPAY", "WEX", "PAGS", "PAYO", "PYPL", "AXP", "FOUR", "STNE", "MA", "EVTC", "V", "WU", "QTWO", "TOST", "GPN", "FIS"],
    "POWER SEMICONDUCTORS": ["NVTS", "VICR", "MPWR", "WOLF", "ON", "STM", "TXN", "NXPI", "SWKS", "MCHP", "DIOD", "AOSL"],
    "PSYCHEDELICS": ["DRUG", "GHRS", "ATAI", "CMPS", "PSIL", "DFTX"],
    "QUANTUM COMPUTING": ["INFQ", "IONQ", "QMCO", "QUBT", "QBTS", "BTQ", "RGTI", "ARQQ", "QTUM"],
    "RAILROAD": ["RAIL", "WAB", "CNI", "TRN", "CSX", "NSC", "UNP", "FSTR", "CP", "GBX"],
    "RARE EARTH": ["CRML", "NB", "PPTA", "UAMY", "USAR", "MP", "METC", "TMC", "MOS"],
    "REGIONAL BANK": ["AX", "WAL", "TBBK", "CUBI", "ZION", "PNFP", "ASB", "TFC", "TCBI", "CATY", "VLY", "FULT", "ABCB", "GBCI", "EWBC", "CFG", "WTFC", "FFIN", "BPOP", "UMBF", "UBSI", "FHB", "FNB", "PFS", "COLB", "FBP", "BANC", "ONB", "HWC", "BKU", "SSB", "BOH", "RF", "OZK", "HBAN", "FHN", "BOKF", "MTB", "CVBF", "CFR", "PB", "CBSH", "FIBK", "WBS", "INDB", "HOMB", "CADE", "SNV"],
    "REIT - SPECIALTY": ["UNIT", "SBAC", "DLR", "EQIX", "OUT", "IRM", "WY", "RYN", "CCI", "LAMR", "EPR", "GLPI", "FPI", "AMT", "LAND", "FRMI"],
    "RESTAURANTS": ["NDLS", "SG", "SHAK", "BLMN", "ARCO", "CNNE", "BJRI", "EAT", "CAKE", "JACK", "CMG", "HCHL", "PZZA", "CBRL", "SBUX", "PTLO", "CAVA", "BROS", "PLAY", "QSR", "DIN", "FWRG", "KRUS", "YUM", "WING", "NATH", "BH", "STKS", "DRI", "LOCO", "MCD", "WEN", "TXRH", "DPZ", "CHA", "RAVE", "YUMC", "BRCB", "PC"],
    "ROBOTICS": ["SYM", "TER", "CRNC", "ROK", "ZBRA", "EMR", "NVDA", "CGNX", "ARBE", "AMBA", "NOVT", "IPGP", "CDNS", "TRMB", "NDSN", "RR", "QCOM", "DE", "MBLY", "TSLA", "SERV", "ISRG", "HLX", "ADSK", "PRCT", "JOBY", "HSAI", "PTC", "AVAV"],
    "SEMICONDUCTORS": ["AEHR", "MRVL", "INTC", "AMKR", "FORM", "AMD", "ON", "MPWR", "STM", "ARM", "AVGO", "ENTG", "TER", "HIMX", "MCHP", "TXN", "KLAC", "ADI", "MKSI", "LRCX", "AMAT", "NXPI", "NVDA", "SWKS", "TSM", "ASML", "CDNS", "QRVO", "SNPS", "OLED", "QCOM", "MU", "SMCI"],
    "SEMIS AI": ["MRVL", "INTC", "AMD", "ARM", "AVGO", "TER", "KLAC", "LRCX", "AMAT", "NVDA", "TSM", "ASML", "CDNS", "SNPS", "MU", "SMCI"],
    "SEMIS NON AI": ["AEHR", "TSEM", "FORM", "ON", "MPWR", "STM", "MCHP", "TXN", "ADI", "AEIS", "NXPI", "SWKS", "QRVO", "OLED", "QCOM"],
    "SOFTWARE": ["PL", "U", "ZM", "TWLO", "RNG", "SE", "MGNI", "FRSH", "SHOP", "DOCN", "PEGA", "IGV", "KVYO", "RBRK", "FROG", "MDB", "CRWD", "S", "NTNX", "BILL", "PLTR", "DDOG", "GTLB", "CLBT", "NET", "ASAN", "ZS", "ESTC", "NOW", "HUBS", "SNOW", "PATH", "BL", "WIX", "FIG"],
    "SOLAR": ["SHLS", "ARRY", "HASI", "DQ", "CWEN", "RNW", "RUN", "FSLR", "NXT", "JKS", "SEDG", "ENPH", "CSIQ"],
    "SPACE": ["SIDU", "SATL", "FLY", "IRDM", "PL", "LUNR", "BKSY", "GSAT", "SATS", "RKLB", "SPCE", "ARKX", "RDW", "HON", "ASTS", "AVAV", "KRMN"],
    "SPECIALTY RETAIL": ["ARKO", "BNED", "HZO", "DKS", "ASO", "PSMT", "EVGO", "TBBB", "CASY", "TGT", "BWMX", "FIVE", "SVV", "ARHS", "WSM", "SBH", "MUSA", "ULTA", "GME", "BBY", "WMT", "RH", "COST", "DG", "BBW", "DLTR", "TSCO", "BBWI", "BJ", "WINA", "MNSO", "OLLI", "WOOF", "EYE", "BOBS"],
    "STARGATE": ["MRAM", "ARM", "ORCL", "NVDA", "MSFT"],
    "STEEL": ["GGB", "ASTL", "CLF", "MTUS", "NUE", "VALE", "MT", "STLD", "SID", "PKX", "NWPX", "RIO", "TX", "RS", "WS", "WS", "CMC", "HCC", "MSB"],
    "TELEHEALTH": ["HIMS", "AMWL", "TDOC", "CVS", "GDRX", "DOCS", "LFMD"],
    "URANIUM AND NUCLEAR": ["NNE", "URG", "GEV", "OKLO", "ASPI", "BWXT", "UEC", "CCJ", "UUUU", "NXE", "SOLS", "DNN", "SMR", "MIR", "UROY", "AMTM", "LEU", "VST", "CEG"],
    "VACCINE": ["BNTX", "MRNA", "PFE", "NVAX"],
}

# Log summary at import
_n_baskets = len(SECTOR_BASKETS)
_n_tickers = sum(len(v) for v in SECTOR_BASKETS.values())
_unique = len({t for tickers in SECTOR_BASKETS.values() for t in tickers})
logger.info("Baskets loaded: %d baskets, %d entries, %d unique tickers", _n_baskets, _n_tickers, _unique)


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
