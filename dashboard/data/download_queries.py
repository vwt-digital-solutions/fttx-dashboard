def sleutel_info_redenna_modal(sleutels):
    """
    This function generates an sql query for a particular set of sleutels
    """
    placeholders = ", ".join(["%s" for _ in sleutels])
    return f"""
select  adres, postcode, huisnummer, soort_bouw, toestemming, creation,
        opleverstatus, opleverdatum, hasdatum, redenna,
        toelichting_status, plan_type, sleutel from fc_aansluitingen
where   sleutel in ({placeholders}) order by creation
"""  # nosec
