from sqlalchemy import text


def sleutel_info_redenna_modal():
    """
    This function generates an sql query for a particular set of sleutels
    """
    return text(
        """
select  adres, postcode, huisnummer, soort_bouw, toestemming, creation,
        opleverstatus, opleverdatum, hasdatum, redenna,
        toelichting_status, plan_type, sleutel from fc_aansluitingen
where   sleutel in (:sleutels) order by creation
"""
    )
