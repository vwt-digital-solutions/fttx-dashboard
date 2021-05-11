from sqlalchemy import text


def sleutel_info_redenna_modal(sleutels: str):
    """
    This function generates an sql query for a particular set of sleutels
    """
    sleutels = sleutels[1:-1]
    return text(
        """
select  adres, postcode, huisnummer, soort_bouw, toestemming, creation,
        opleverstatus, opleverdatum, hasdatum, redenna,
        toelichting_status, plan_type, sleutel from fc_aansluitingen
where   sleutel in (:sleutels) order by creation
"""
    ).bindparams(sleutels=sleutels)
