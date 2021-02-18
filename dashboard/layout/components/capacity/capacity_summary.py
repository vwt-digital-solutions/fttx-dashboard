import dash_html_components as html
from layout.components.indicator import indicator


def capacity_summary(phase_name, target, work_stock, capacity, poc, unit):
    """
    The capacity summary component shows the main indicators regarding the capacity analysis.

    Args:
        phase_name (str): The phase used in the title.
        target:
        work_stock:
        capacity:
        poc:

    Returns:
        html.Div
    """
    return [
        html.Div(
            className="container-display",
            children=html.H2(f"Capaciteit voor {phase_name}"),
        ),
        html.Div(
            className="container-display",
            children=[
                indicator(value=capacity,
                          title="Wat ga ik doen?",
                          suffix=unit),
                indicator(value=target,
                          title="Wat heb ik afgesproken?",
                          suffix=unit),
                indicator(value=work_stock,
                          title="Wat kan ik doen?",
                          suffix=unit),
                indicator(value=poc,
                          title="Wat moet ik doen?",
                          suffix=unit)
            ]
        )
    ]
