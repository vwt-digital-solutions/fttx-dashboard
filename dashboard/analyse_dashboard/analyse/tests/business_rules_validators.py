import business_rules as br


def validate_bis_niet_opgeleverd(df):
    result = br.bis_niet_opgeleverd(df).sum()
    result_assumed = df.opleverstatus.value_counts().get("0", 0)
    assert result == result_assumed


def validate_bis_opgeleverd(df):
    result = br.bis_opgeleverd(df).sum()
    result_assumed = len(df.opleverstatus) - df.opleverstatus.value_counts().get("0", 0)
    assert result == result_assumed


def validate_laswerk_dp_niet_gereed(df):
    result = br.laswerk_dp_niet_gereed(df).sum()
    result_assumed = len(df.laswerkdpgereed) - df.laswerkdpgereed.value_counts().get("1", 0)
    assert result == result_assumed


def validate_laswerk_dp_gereed(df):
    result = br.laswerk_dp_gereed(df).sum()
    result_assumed = df.laswerkdpgereed.value_counts().get("1", 0)
    assert result == result_assumed


def validate_laswerk_ap_niet_gereed(df):
    result = br.laswerk_ap_niet_gereed(df).sum()
    result_assumed = len(df.laswerkapgereed) - df.laswerkapgereed.value_counts().get("1", 0)
    assert result == result_assumed


def validate_laswerk_ap_gereed(df):
    result = br.laswerk_ap_gereed(df).sum()
    result_assumed = df.laswerkapgereed.value_counts().get("1", 0)
    assert result == result_assumed


def validate_geschouwed(df):
    br.geschouwed()
