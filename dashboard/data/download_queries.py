from sqlalchemy import text
import config
import inspect


def validate_project(func):
    """
    A decorator that checks if the value of the project argument is one of the configured projects

    :raises ValueError: When a the value of the project argument is not in one of the configured projects a ValueError
     is raised
    :param func: the decorated function
    :return: The original function
    """

    def wrapper(*args, **kwargs):
        # Gets a dictionary of all supplied arguments and their values
        all_args = {**dict(zip(inspect.getfullargspec(func).args, args)), **kwargs}

        projects = config.projects_dfn + config.projects_tmobile + config.subset_KPN_2021

        project = all_args.get("project")
        if project not in projects:
            raise ValueError(f"Unknown project supplied: {project}")
        return func(*args, **kwargs)

    wrapper.__doc__ = func.__doc__
    return wrapper


@validate_project
def waiting_category(project: str, wait_category: str, order_type: str):
    """
    This function generates an sql query for a particular project that returns the houses in a particular waiting
    category. See the BR openstaande_orders_tmobile for the logic behind the SQL query

    :param project: A fiberconnect project name
    :param wait_category: The wait category used in the filter. One of [on_time, limited_time, late]
    :return: A parameterized SQL-query
    """
    if wait_category == "on_time":
        having = "wachttijd > 0 and wachttijd <= 8"
    elif wait_category == "limited":
        having = "wachttijd > 8 and wachttijd <= 12"
    elif wait_category == "late":
        having = "wachttijd > 12"
    else:
        having = "wachttijd > 0"

    if order_type == "hc_aanleg":
        plan_type = "not in ('Zonder klantafspraak')"
    elif order_type == "patch_only":
        plan_type = "in ('Zonder klantafspraak')"

    return text(f"""
Select  fc.adres, fc.postcode, fc.huisnummer, fc.soort_bouw, fc.toestemming, fc.creation as creationdatum,
        fc.opleverstatus, fc.opleverdatum, fc.hasdatum, f.cluster_redenna, fc.redenna,
        fc.toelichting_status, fc.plan_type, DATEDIFF(NOW(), fc.creation)/7 as wachttijd
from fc_aansluitingen fc
left join fc_clusterredenna f on fc.redenna = f.redenna
where   fc.project = :project
and     fc.status not in ('CANCELLED', 'TO_BE_CANCELLED', 'CLOSED')
and     fc.type in ('AANLEG', 'Aanleg')
and     fc.plan_type {plan_type}
having {having}
order by fc.creation
    """).bindparams(project=project)  # nosec


@validate_project
def project_redenna(project,
                    schouw_status=None,
                    bis_status=None,
                    lasap_status=None,
                    lasdp_status=None,
                    has_status=None
                    ):
    """
    This function generates an sql query for a particular project with optional filters for the phases (Schouwen,
    BIS, lasAP, lasDP HAS).

    :param project: A fiberconnect project name
    :param schouw_status: One of [niet_opgeleverd, opgeleverd]
    :param bis_status: One of [niet_opgeleverd, opgeleverd]
    :param lasap_status: One of [niet_opgeleverd, opgeleverd]
    :param lasdp_status: One of [niet_opgeleverd, opgeleverd]
    :param has_status: One of [niet_opgeleverd, ingeplanned, opgeleverd_zonder_hc, opgeleverd]
    :return: A parameterized SQL-query
    """
    filters = ""
    if schouw_status:
        if schouw_status == "niet_opgeleverd":
            filters += "and fc.toestemming is not null\n"
        elif schouw_status == "opgeleverd":
            filters += "and fc.toestemming is null\n"
    if bis_status:
        if bis_status == "niet_opgeleverd":  # see BR: bis_niet_opgeleverd
            filters += "and fc.opleverstatus in (0, 90, 99)\n"
        elif bis_status == "opgeleverd":  # see BR: bis_opgeleverd
            filters += "and fc.opleverstatus not in (0, 90, 99)\n"
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
    Select
        fc.sleutel,
        fc.project,
        fc.plaats, fc.postcode, fc.adres, fc.huisnummer,
        f.cluster_redenna, fc.redenna, fc.toelichting_status,
        fc.soort_bouw,
        fc.schouwdatum,
        fc.laswerkdpgereed,
        fc.laswerkapgereed,
        fc.opleverstatus, fc.opleverdatum,
        fc.hasdatum,
        fc.creation as creationdatum
from fc_aansluitingen as fc
left join fc_clusterredenna f on fc.redenna = f.redenna
where project = :project
{filters}
    """).bindparams(project=project)  # nosec
    return sql
