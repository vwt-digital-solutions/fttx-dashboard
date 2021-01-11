from sqlalchemy import text
import config
import inspect


def validate_project(func):
    def wrapper(*args, **kwargs):
        # Gets a dictionary of all supplied arguments and their values
        all_args = {**dict(zip(inspect.getfullargspec(func).args, args)), **kwargs}

        projects = config.projects_dfn + config.projects_tmobile + config.subset_KPN_2020

        project = all_args.get("project")
        if project not in projects:
            raise ValueError(f"Unknown project supplied: {project}")
        return func(*args, **kwargs)

    return wrapper


@validate_project
def waiting_category(project, wait_category):
    if wait_category == "on_time":
        having = "wachttijd > 0 and wachttijd <= 8"
    elif wait_category == "limited_time":
        having = "wachttijd > 8 and wachttijd <= 12"
    elif wait_category == "late":
        having = "wachttijd > 12"
    else:
        having = "wachttijd > 0"

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


@validate_project
def project_redenna(project,
                    schouw_status=None,
                    bis_status=None,
                    lasap_status=None,
                    lasdp_status=None,
                    has_status=None
                    ):
    filters = ""
    if schouw_status:
        if schouw_status == "niet_opgeleverd":
            filters += "and fc.toestemming is not null\n"
        elif schouw_status == "opgeleverd":
            filters += "and fc.toestemming is null\n"
    if bis_status:
        if bis_status == "niet_opgeleverd":
            filters += "and fc.opleverstatus = 0\n"
        elif bis_status == "opgeleverd":
            filters += "and fc.opleverstatus != 0\n"
    if lasdp_status:
        if lasdp_status == "niet_opgeleverd":
            filters += "and fc.laswerkdpgereed != 1\n"
        elif lasdp_status == "opgeleverd":
            filters += "and fc.laswerkdpgereed = 1\n"
    if lasap_status:
        if lasap_status == "niet_opgeleverd":
            filters += "and fc.laswerkapgereed != 1\n"
        elif lasap_status == "opgeleverd":
            filters += "and fc.laswerkapgereed = 1\n"
    if has_status:
        if has_status == "niet_opgeleverd":
            filters += "and fc.opleverdatum is null and fc.hasdatum is null\n"
        elif has_status == "ingeplanned":
            filters += "and fc.opleverdatum is null and fc.hasdatum is not null\n"
        elif has_status == "opgeleverd_zonder_hc":
            filters += "and fc.opleverstatus != 2 and fc.opleverdatum is not null\n"
        elif has_status == "opgeleverd":
            filters += "and fc.opleverstatus = 2\n"

    sql = text(f"""
    Select fc.project,
        f.cluster_redenna, fc.redenna, fc.toelichting_status,
        fc.soort_bouw,
        fc.schouwdatum,
        fc.laswerkdpgereed,
        fc.laswerkapgereed,
        fc.opleverstatus, fc.opleverdatum,
        fc.hasdatum
from fc_aansluitingen as fc
left join fc_clusterredenna f on fc.redenna = f.redenna
where project = :project
{filters}
    """).bindparams(project=project)  # nosec
    return sql
