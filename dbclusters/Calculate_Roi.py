#ROI
import pandas as pd
import plotly.express as px
import warnings
warnings.filterwarnings("ignore")

def calculate(weeks = 52, auto_scaling = 4, idle_time = 2, active_clusters = 3, active_time = 4, dbu_costs = 0.4):
    '''
    Function to plot cumulative saved dollars on a weekly level.

    Parameters:
    weeks (int): Number of weeks (default is 52).
    auto_scaling (int): Number of auto-scaling instances (default is 4).
    idle_time (int): Number of idle time (default is 2).
    active_clusters (int): Number of active clusters (default is 3).
    active_time (int): Number of active time (default is 4).
    dbu_costs (float): DBU costs (default is 0.4).

    Returns:
    None (displays the plot).
    '''
    
    # Calculate the SavedDollars for each week
    saved_dollars = (auto_scaling * active_clusters * active_time + idle_time * active_clusters) * dbu_costs

    # Create lists to store cumulative saved amounts and week labels
    cumulative_saved = [0]
    week_labels = list(range(0, weeks + 1))  # Start from week 0

    for i in range(weeks):
        cumulative_saved.append(cumulative_saved[i] + saved_dollars)

    # Calculate the differences in saved dollars
    weekly_differences = [cumulative_saved[i] - cumulative_saved[i - 1] for i in range(1, len(cumulative_saved))]

    # Create a DataFrame from the data
    data = pd.DataFrame({'Week': week_labels,
                         'AutoScaling': [auto_scaling] * (weeks + 1),
                         'IdleTime': [idle_time] * (weeks + 1),
                         'ActiveClusters': [active_clusters] * (weeks + 1),
                         'ActiveTime': [active_time] * (weeks + 1),
                         'DBUCosts': [dbu_costs] * (weeks + 1),
                         'SavedDollars': cumulative_saved,
                         'WeeklyDifference': [None] + weekly_differences})

    # Remove the row for week 0 from the DataFrame
    data = data[data['Week'] != 0]

    # Create a Plotly figure
    fig = px.bar(data, x='Week', y='SavedDollars', labels={'Week': 'Week', 'SavedDollars': 'Cumulative Saved Dollars'})

    # Update the layout for better appearance
    fig.update_layout(
        title='Cumulative Saved Dollars on a Weekly Level',
        xaxis_title='Week',
        yaxis_title='Cumulative Saved Dollars',
        xaxis=dict(tickmode='array', tickvals=week_labels[1:], ticktext=week_labels[1:]),
        xaxis_tickangle=-45,
        margin=dict(l=40, r=20, t=80, b=40),
        showlegend=False)

    # Show the Plotly figure
    fig.show()