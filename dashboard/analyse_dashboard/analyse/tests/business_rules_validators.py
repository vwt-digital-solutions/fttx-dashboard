import business_rules as br


def validate_bis_opgeleverd(df):
    result = br.bis_opgeleverd(df).sum()
    result_assumed = len(df.opleverstatus) - df.opleverstatus.value_counts().get("0", 0)
    assert result == result_assumed


def validate_laswerk_dp_gereed(df):
    result = br.laswerk_dp_gereed(df).sum()
    result_assumed = df.laswerkdpgereed.value_counts().get("1", 0)
    assert result == result_assumed


def validate_laswerk_ap_gereed(df):
    result = br.laswerk_ap_gereed(df).sum()
    result_assumed = df.laswerkapgereed.value_counts().get("1", 0)
    assert result == result_assumed
