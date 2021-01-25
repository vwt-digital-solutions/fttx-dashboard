import dash_html_components as html
from layout.components.indicator import indicator


def capacity_summary(phase):
    """
    The capacity summary component shows the 4 main indicators regarding the capacity analysis.

    Args:
        phase (str): The phase used in the title.

    Returns:
        html.Div
    """
    return [
        html.Div(
            className="container-display",
            children=html.H2(f"Capaciteit voor {phase}"),
        ),
        html.Div(
            className="container-display",
            children=[
                indicator(value=500,
                          previous_value=480,
                          title="Wat ga ik doen?"),
                indicator(value=500,
                          previous_value=480,
                          title="Wat heb ik afgesproken?"),
                indicator(value=500,
                          previous_value=480,
                          title="Wat kan ik doen?"),
                indicator(value=500,
                          previous_value=480,
                          title="Wat moet ik doen?")

            ]
        )
    ]
