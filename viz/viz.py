from collections import Counter
from ipaddress import ip_address, IPv4Address
from scipy import stats
import boto3
import dash
import dash_core_components as dcc
import dash_html_components as html
import glob
import io
import json
import numpy as np
import pandas
import pandas as pd
import plotly
import plotly as py
import plotly.express as px
import plotly.graph_objs as go
import pprint
import re
import siphash
import uuid

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


def draw_hit_count(df: pandas.core.frame.DataFrame):

  scat = go.Scattergl(
    x=df.time_window,
    y=df.event_count,
    showlegend = False,
    hoverinfo='all'
  )

  fig = go.Figure(scat)
  fig.update_layout(
    title={
      'text': 'Number of HTTP requests over time',
      'y':0.9,
      'x':0.5,
      'xanchor': 'center',
      'yanchor': 'top'},
    xaxis_title='Time',
    yaxis_title="[http req]")
  fig.show()


def valid_ip_address(ip: str) -> str:
  try:
      return "IPv4" if type(ip_address(ip)) is IPv4Address else "IPv6"
  except ValueError:
      return "Invalid"


def get_siphash(s, key):
  sip = siphash.SipHash_2_4(key)
  sip.update(bytes(s, 'utf8'))
  return sip.hexdigest().decode('utf-8')


def generate_aggregates(df_input, fields, cnt_field):
  df_return = df_input.groupby(fields)[cnt_field].count().reset_index()
  df_return.rename(columns={cnt_field:'Cnt'}, errors='raise', inplace=True)
  df_return = df_return.loc[:,~df_return.columns.duplicated()]
  return df_return

def convert_to_time_indexed(df, index_column):
    df.set_index(index_column, inplace=True)
    return df

def visitor_heatmap(df, scale='lin'):

  dfa = convert_to_time_indexed(generate_aggregates(df, ['day', 'hour'], 'ScStatus'), 'day')

  size_lin = dfa.Cnt.values
  size_log_2 = np.log(dfa.Cnt.values) / np.log(2)

  size = size_lin if scale=='lin' else size_log_2

  fig = go.Figure(
    data=go.Scattergl(
      x=dfa.index,
      y=dfa['hour'],
      mode='markers',
      marker=dict(
        color=dfa.Cnt,
        colorscale = 'portland',
        line_width=1,
        size=size,
        showscale=True,
        sizemin=3
      )
    )
  )

  fig.update_layout(
    height=600,
    title_text='Number of request per hour over time',
    yaxis_nticks=24,
    xaxis_nticks=31
  )

  fig.update_yaxes(autorange="reversed")
  return fig

def get_s3_client():
  session = boto3.Session(profile_name='li-istvan', region_name='eu-west-1')
  return session.client('s3')


def get_files_on_s3():
  s3_client = get_s3_client()
  s3_objects = s3_client.list_objects_v2(
    Bucket='logs.l1x.be',
    Prefix ='dwh/web-logs/',
    MaxKeys=100)

  return s3_objects


def read_parquet_files_from_s3(s3_files):
  s3_client = get_s3_client()
  months_raw = []
  for obj in s3_files['Contents']:
    print(obj['Key'])
    s3_obj = s3_client.get_object(Bucket='logs.l1x.be', Key=obj['Key'])
    df = pd.read_parquet(io.BytesIO(s3_obj['Body'].read()))
    months_raw.append(df)
  return months_raw


def get_reports(months_raw):
  months_report = []
  seed =  uuid.uuid4().bytes

  for df in months_raw:
    print(df.shape)

    df_tmp = df.copy()
    df_tmp['CsUriStreamClean']  = df_tmp['CsUriStem'].apply(lambda s: s.rstrip('/'))
    df_tmp['IATA'] = df_tmp.apply(lambda x: x['EdgeLocation'][0:3],axis=1)
    df_tmp['DateTime'] =  pd.to_datetime(df_tmp['DateTime'],infer_datetime_format=True)
    df_tmp['hour'] = df_tmp['DateTime'].dt.floor('H').dt.hour
    df_tmp['day'] = df_tmp['DateTime'].dt.date
    df_tmp['ipv'] = df_tmp.apply(lambda x: valid_ip_address(x['CIp']),axis=1)
    df_tmp['hcip'] = df_tmp.apply(lambda x: get_siphash(x['CIp'], seed),axis=1)
    df_ready = df_tmp[
        ['DateTime', 'day','hour','IATA','CsMethod',
        'CsUriStreamClean', 'CsUriStem',
        'ScStatus', 'ScBytes','CsProtocol',
        'CsProtocolVersion', 'TimeToFirstByte',
        'XEdgeDetailedResultType', 'ScContentLen',
        'SslProtocol' ,'SslCipher', 'ipv',
        'CsReferer', 'hcip']
    ]
    months_report.append(df_ready)
  return months_report


def generate_table(dataframe, max_rows=15):
  return html.Table([
    html.Thead(
      html.Tr([html.Th(col) for col in dataframe.columns])
    ),
    html.Tbody([
      html.Tr([
          html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
      ]) for i in range(min(len(dataframe), max_rows))
    ])
  ])


def get_top_referers(months_report):
  top_referers = []
  for df in months_report:
    df_ref = df[df['CsReferer'] != '-']
    filter_self = df_ref['CsReferer'].str.contains('dev\\.l1x\\.be')
    df_ref = df_ref[~filter_self]
    top_ref = df_ref.groupby(['CsReferer'])['CsReferer'].count().nlargest(10).to_frame()
    top_ref.rename(columns={'CsReferer':'Cnt'}, errors='raise', inplace=True)
    top_ref.reset_index(level=0, inplace=True)
    top_referers.append(top_ref)
  return top_referers


def get_top_posts(months_report):
  top_urls = []
  for df in months_report:
    df = df[df['CsUriStreamClean'].str.contains('posts')].copy()
    top_url = df.groupby(['CsUriStreamClean'])['CsUriStreamClean'].count().nlargest(10).to_frame()
    top_url.rename(columns={'CsUriStreamClean':'Cnt'}, errors='raise', inplace=True)
    top_url.reset_index(level=0, inplace=True)
    top_urls.append(top_url)
  return top_urls


def get_iata_codes(months_report):
  top_iata = []
  for df in months_report:
    data = df.groupby(['IATA'])['hcip'].count().nlargest(30).to_frame()
    fig = px.bar(data, x=data.index, y=data.hcip)
    top_iata.append(fig)
  return top_iata


def get_visiting_times(months_report):
  times = []
  for df in months_report:
    fig = visitor_heatmap(df, 'log')
    times.append(fig)
  return times


def create_layout():
  s3_files = get_files_on_s3()
  s3_file_names = [ obj['Key'] for obj in    s3_files['Contents']]
  months_raw = read_parquet_files_from_s3(s3_files)
  months_report = get_reports(months_raw)

  print('Loaded data...')

  idx = 0
  top_refs = []
  top_refs.append(html.H3('Top Referers'))
  for top_ref in get_top_referers(months_report):
    children = []
    children.append(html.H4(s3_file_names[idx]))
    children.append(generate_table(top_ref))
    top_refs.append(html.Div(children, className='row'))
    idx += 1

  idx = 0
  top_posts = []
  top_posts.append(html.H3('Top Posts'))
  for top_url in get_top_posts(months_report):
    children = []
    children.append(html.H4(s3_file_names[idx]))
    children.append(generate_table(top_url))
    top_posts.append(html.Div(children, className='row'))
    idx += 1

  idx = 0
  top_iatas = []
  top_iatas.append(html.H3('Top IATAs'))
  for top_iata in get_iata_codes(months_report):
    children = []
    children.append(html.H4(s3_file_names[idx]))
    children.append(dcc.Graph(
        id='Top IATA codes where readers are {}'.format(idx),
        figure=top_iata
    ))
    top_iatas.append(html.Div(children, className='row'))
    idx += 1

  idx = 0
  times = []
  times.append(html.H3('Visiting Times'))
  for time_fig in get_visiting_times(months_report):
    children = []
    children.append(html.H4(s3_file_names[idx]))
    children.append(dcc.Graph(
        id='Hit distribution over time {}'.format(idx),
        figure=time_fig
    ))
    times.append(html.Div(children, className='row'))
    idx += 1


  app.layout = html.Div([
      html.Div(top_refs, className='three columns', style={'margin-top': '2em'}),
      html.Div(top_posts, className='three columns', style={'margin-top': '2em'}),
      html.Div(top_iatas, className='three columns', style={'margin-top': '2em'}),
      html.Div(times, className='three columns', style={'margin-top': '2em'}),
      html.Div(html.H4('The end'), className='three columns')
    ])



  return 'ok'




if __name__ == '__main__':
  create_layout()
  app.run_server(debug=True)












# len(months_raw)


  # dfa = generate_aggregates(months_report[5], ['day', 'hour'], 'ScStatus')
  # dfa.set_index('day', inplace=True)

  # return dfa

# months_report = []
# seed =  uuid.uuid4().bytes
# for df in months_raw:
#     print(df.shape)
#     df_posts = df[df['CsUriStem'].str.contains('posts')].copy()
#     df_posts['CsUriStreamClean']  = df_posts['CsUriStem'].apply(lambda s: s.rstrip('/'))
#     df_posts['IATA'] = df_posts.apply(lambda x: x['EdgeLocation'][0:3],axis=1)
#     df_posts['DateTime'] =  pd.to_datetime(df_posts['DateTime'],infer_datetime_format=True)
#     df_posts['hour'] = df_posts['DateTime'].dt.floor('H').dt.hour
#     df_posts['day'] = df_posts['DateTime'].dt.date
#     df_posts['ipv'] = df_posts.apply(lambda x: validIPAddress(x['CIp']),axis=1)
#     df_posts['hcip'] = df_posts.apply(lambda x: get_siphash(x['CIp'], seed),axis=1)
#     df_ready = df_posts[
#         ['DateTime', 'day','hour','IATA','CsMethod',
#          'CsUriStreamClean', 'ScStatus', 'ScBytes','CsProtocol',
#          'CsProtocolVersion', 'TimeToFirstByte',
#          'XEdgeDetailedResultType', 'ScContentLen',
#          'SslProtocol' ,'SslCipher', 'ipv',
#         'CsReferer', 'hcip']
#     ]
#     months_report.append(df_ready)
# len(months_report)

# df = months_report[4]
# df_ref = df[df['CsReferer'] != '-']
# df_ref.groupby(['CsReferer'])['CsReferer'].count().nlargest(5).to_frame()


# df.groupby(['CsUriStreamClean'])['CsUriStreamClean'].count().nlargest(5).to_frame()


# data = df.groupby(['IATA'])['hcip'].count().nlargest(30).to_frame()
# fig = px.bar(data, x=data.index, y=data.hcip)
# fig.show()

# fig = visitor_heatmap(df, 'log')
# fig.show()


