from sqlalchemy import text
import config


def waiting_category(project, wait_category):
    if wait_category == "on_time":
        having = "wachttijd > 0 and wachttijd <= 8"
    elif wait_category == "limited_time":
        having = "wachttijd > 8 and wachttijd <= 12"
    elif wait_category == "late":
        having = "wachttijd > 12"
    else:
        having = "wachttijd > 0"

    projects = config.projects_dfn + config.projects_tmobile + config.subset_KPN_2020

    if project not in projects:
        raise ValueError(f"Unknown project supplied: {project}")

    return text(f"""
Select  fc.adres, fc.postcode, fc.huisnummer, fc.soort_bouw, fc.toestemming,
        fc.toestemming_datum, fc.opleverstatus, fc.opleverdatum, fc.hasdatum, f.cluster_redenna, fc.redenna,
        fc.toelichting_status, DATEDIFF(NOW(), fc.toestemming_datum)/7 as wachttijd
from fc_aansluitingen fc
left join fc_clusterredenna f on fc.redenna = f.redenna
where   fc.project = :project
and     fc.opleverdatum is null
and     fc.toestemming is not null
having {having}
order by fc.toestemming_datum
    """).bindparams(project=project)  # nosec
