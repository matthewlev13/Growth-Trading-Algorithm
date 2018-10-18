from quantopian.algorithm import attach_pipeline,pipeline_output
from quantopian.pipeline import Pipeline
from quantopian.pipeline.data.builtin import USEquityPricing
from quantopian.pipeline.filters import Q500US
from quantopian.pipeline.data import morningstar


def initialize(context):

  schedule_function(my_rebalance,date_rules.week_start(),time_rules.market_open(hours=1))

    my_pipe = make_pipeline()
    attach_pipeline(my_pipe,'my_pipeline')


def make_pipeline():

    # Base Universe.
    base_universe = Q500US()


    # Define Sectors of Assumed Growth
    sector = morningstar.asset_classification.morningstar_sector_code.latest
    tech_sector = sector.eq(311)
    healthcare_sector = sector.eq(206)



    # Revenue Growth
    revenue_growth = morningstar.operation_ratios.revenue_growth.latest
    high_revenue_growth = revenue_growth.percentile_between(80,100,mask = base_universe)


    # Return on Invested Capital
    roic = morningstar.operation_ratios.roic.latest
    high_roic = roic.percentile_between(80,100,mask = high_revenue_growth)


    # Filtering ROIC as growth adds value
    long_value_roic = high_roic #> wacc


    # Filters
    high_growth = (long_value_roic & high_revenue_growth & (tech_sector | healthcare_sector))
    securities_to_trade = high_growth


    return Pipeline(
        columns = {
            'Sector': sector,
            'Revenue Growth': revenue_growth,
            'ROIC': roic,
            'High Growth': high_growth,
        }, screen = securities_to_trade)



def my_rebalance(context,data):

    # S&P500 returns
    spy = sid(8554)


    for security in context.portfolio.positions:
        if security not in context.longs and data.can_trade(security):
            order_target_percent(security,0)

    for security in context.longs:
        if data.can_trade(security):
            order_target_percent(security,context.long_weight)


def my_compute_weights(context):

    if len(context.longs)==0:
        long_weight = 0
    else:
        long_weight = 0.8 / len(context.longs)

    return long_weight


def before_trading_start(context,data):
    context.output = pipeline_output('my_pipeline')

    # Long
    context.longs = context.output[context.output['High Growth']].index.tolist()


    context.long_weight = my_compute_weights(context)
