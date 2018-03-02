from bokeh.plotting import figure, save, output_file, ColumnDataSource, gridplot
from bokeh.models import HoverTool
import pandas as pd
import sys
import getopt
from meterman import meter_data_manager as mdata_mgr, meter_db as db, app_base as base


def output_plot(node_uuid, plot_output_file, data_mgr=None, db_file=None):
    if data_mgr is None and db_file is None:
        raise ValueError('Must supply a data mgr instance or a path to a db file.')
    if data_mgr is None:
        data_mgr = mdata_mgr.MeterDataManager(db_file=db_file, log_file='/dev/null')

    # Get all meter entries and prep dataframe
    meter_entries = data_mgr.get_meter_entries(node_uuid=node_uuid, entry_type=None, rec_status=db.RecStatus.NORMAL.value, time_from=None, time_to=None, limit_count=None)

    if len(meter_entries) == 0:
        print('No Data.  Exiting...')
        return

    df = pd.DataFrame.from_records(meter_entries)
    df['when_finish_dt_utc'] = pd.to_datetime(df['when_start'] + df['duration'], unit='s').dt.tz_localize('UTC')
    # df.set_index(['when_finish_dt_utc', 'node_uuid', 'entry_type', 'when_start_raw_nonce'])
    df['when_start_dt_local'] = pd.to_datetime(df['when_start'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Australia/Melbourne').dt.tz_localize(None) # hack given bokeh bug
    df['when_finish_dt_local'] = df['when_finish_dt_utc'].dt.tz_convert('Australia/Melbourne').dt.tz_localize(None) # hack given bokeh bug
    df['duration_scaled'] = df['duration'] * 1000 * .9
    df['when_midinterval_dt_local'] = pd.to_datetime(df['when_start'] + (df['duration']/2), unit='s').dt.tz_localize('UTC').dt.tz_convert('Australia/Melbourne').dt.tz_localize(None) # hack given bokeh bug
    df['mup_value'] = df.apply(lambda row: row.meter_value if row.entry_type in [db.EntryType.METER_REBASE.value, db.EntryType.METER_REBASE_SYNTH.value] else None, axis=1)

    source = ColumnDataSource(df)

    p_cumulative = figure(title="Cumulative Consumption for node {} from {} to {}".format(node_uuid, df['when_finish_dt_local'].min(), df['when_finish_dt_local'].max()),
                x_axis_label='Date/Time (Melbourne)', y_axis_label='Consumption (Wh)',
                x_axis_type = "datetime",
                tools = "xpan,xwheel_zoom,reset,hover", active_drag='xpan', active_scroll='xwheel_zoom')


    p_cumulative.step(x='when_finish_dt_local', y='meter_value', legend="Accumulated Consumption", line_width=2, source=source, mode="after")
    p_cumulative.inverted_triangle(x='when_finish_dt_local', y='mup_value', legend="Meter Rebase", size=20, color="#DE2D26", source=source)

    p_interval = figure(title="Interval Consumption",
                x_axis_label='Date/Time (Melbourne)', y_axis_label='Consumption (Wh)',
                x_axis_type = "datetime", x_range=p_cumulative.x_range,
                tools = "xpan,xwheel_zoom,reset,hover", active_drag='xpan', active_scroll='xwheel_zoom')


    p_interval.vbar(x='when_midinterval_dt_local', bottom=0, top='entry_value', width='duration_scaled', color="navy", source=source)

    hover_tooltips = [
        ("#", "$index"),
        ("Interval Start (Local)", "@when_start_dt_local{%Y-%m-%d %H:%M:%S}"),
        ("Interval Start (UTC)", "@when_start"),
        ("Interval Finish (Local)", "@when_finish_dt_local{%Y-%m-%d %H:%M:%S}"),
        ("Interval Duration", "@duration s"),
        ("Interval Consumption", "@entry_value Wh"),
        ("Accumulated Consumption", "@meter_value Wh"),
        ("Entry Type", "@entry_type"),
    ]

    hover_formatters = {
        'when_start_dt_local': 'datetime',
        'when_finish_dt_local': 'datetime'
    }

    hover = p_cumulative.select(dict(type=HoverTool))
    hover.tooltips = hover_tooltips
    hover.formatters = hover_formatters

    hover = p_interval.select(dict(type=HoverTool))
    hover.tooltips = hover_tooltips
    hover.formatters = hover_formatters

    p_combined = gridplot([[p_cumulative], [p_interval]], sizing_mode="fixed", plot_width=1200, plot_height=375)

    output_file(plot_output_file)
    save(p_combined)


def main(argv):
    usage_help = 'Usage: viz_data.py -n <node_uuid> -d <db_file> -o <outputfile>'
    try:
        opts, args = getopt.getopt(argv, "hn:d:o:", ["node_uuid=", "db_file=", "output_file"])
        node_uuid = None
        db_file = None
        output_file = None

        for opt, arg in opts:
            if opt == '-h':
                print(usage_help)
                sys.exit()
            elif opt in ("-n", "--node_uuid"):
                node_uuid = arg
            elif opt in ("-d", "--db_file"):
                db_file = arg
            elif opt in ("-o", "--output_file"):
                output_file = arg

        if node_uuid is not None and db_file is not None and output_file is not None:
            output_plot(node_uuid=node_uuid, db_file=db_file, plot_output_file=output_file)
        else:
            print(usage_help)

    except getopt.GetoptError:
        print(usage_help)
        sys.exit(2)


if __name__ == '__main__':
    main(sys.argv[1:])