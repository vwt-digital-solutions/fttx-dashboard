import dash_html_components as html
from layout.components.indicator import indicator


def capacity_summary(phase_name, target, werkvoorraad, capacity, poc):
    """
    The capacity summary component shows the main indicators regarding the capacity analysis.

    Args:
        phase_name (str): The phase used in the title.
        target:
        werkvoorraad:
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
                          title="Wat ga ik doen?"),
                indicator(value=target,
                          title="Wat heb ik afgesproken?"),
                indicator(value=werkvoorraad,
                          title="Wat kan ik doen?"),
                indicator(value=poc,
                          title="Wat moet ik doen?")

            ]
        )
    ]
