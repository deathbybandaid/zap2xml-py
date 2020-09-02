#!/usr/bin/env python
# pylama:ignore=E722
"""zap2xml.py -- The simplest zap2it scraper I could write.

Around June 2020 the `zap2xml.pl` I had stopped working.  It generated HTTP
requests that gave only 400 responses.  I tried to patch it, to the point that
it got OK responses, but parsed no data from them.  The zap2it site must have
changed.  I thought they had an API, but apparently this tool has always
scraped the internal JSON feed, intended just for the web site?

So re-write from scratch.  Simplest possible form I can, so the fewest things
need to change if the site ever does again.  The goal is to feed guide data
into Tvheadend.

The zap2it site, at least for my area/OTA, will give "400 Bad Request" errors
*for certain times* of certain days.  Even their own site does this!  This is
the error that recently started tripping up `zap2xml.pl`.  So this tool simply
ignores 400 errors, continuing to gather the data available for other time
windows.

Written to have only standard library dependencies.
"""

import argparse
import datetime
import json
import pathlib
import sys
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET


def get_args():
    parser = argparse.ArgumentParser(
                description='Fetch TV data from zap2it.',
                epilog='This tool is noisy to stdout; '
                'with cron use chronic from moreutils.')
    parser.add_argument(
          '--aid', dest='zap_aid', type=str, default='gapzap',
          help='Raw zap2it input parameter.  (Affiliate ID?)')
    parser.add_argument(
          '-c', '--country', dest='zap_country', type=str, default='USA',
          help='Country identifying the listings to fetch.')
    parser.add_argument(
          '-d', '--delay', dest='delay', type=int, default=5,
          help='Delay, in seconds, between server fetches.')
    parser.add_argument(
          '--device', dest='zap_device', type=str, default='-',
          help='Raw zap2it input parameter.  (?)')
    parser.add_argument(
          '--headend-id', dest='zap_headendId', type=str, default='lineupId',
          help='Raw zap2it input parameter.  (?)')
    parser.add_argument(
          '--is-override', dest='zap_isOverride', type=bool, default=True,
          help='Raw zap2it input parameter.  (?)')
    parser.add_argument(
          '--language', dest='zap_languagecode', type=str, default='en',
          help='Raw zap2it input parameter.  (Language.)')
    parser.add_argument(
          '--pref', dest='zap_pref', type=str, default='',
          help='Raw zap2it input parameter.  (Preferences?)')
    parser.add_argument(
          '--timespan', dest='zap_timespan', type=int, default=3,
          help='Raw zap2it input parameter.  (Hours of data per fetch?)')
    parser.add_argument(
          '--timezone', dest='zap_timezone', type=str, default='',
          help='Raw zap2it input parameter.  (Time zone?)')
    parser.add_argument(
          '--user-id', dest='zap_userId', type=str, default='-',
          help='Raw zap2it input parameter.  (?)')
    parser.add_argument(
          '-z', '--zip', '--postal', dest='zap_postalCode', type=str, required=True,
          help='The zip/postal code identifying the listings to fetch.')
    parser.add_argument(
          '--output_file', dest='output_file', type=str,
          default=pathlib.Path(__file__).parent.joinpath('xmltv.xml'),
          help='Output File parameter.  (?)')
    return parser.parse_args()


def get_cached(cache_dir, cache_key, delay, url):
    cache_path = cache_dir.joinpath(cache_key)
    if cache_path.is_file():
        # print('FROM CACHE:', url)
        with open(cache_path, 'rb') as f:
            return f.read()
    else:
        # print('Fetching:  ', url)
        try:
            resp = urllib.request.urlopen(url)
            result = resp.read()
        except urllib.error.HTTPError as e:
            if e.code == 400:
                # print('Got a 400 error!  Ignoring it.')
                result = (
                    b'{'
                    b'"note": "Got a 400 error at this time, skipping.",'
                    b'"channels": []'
                    b'}')
            else:
                raise
        with open(cache_path, 'wb') as f:
            f.write(result)
        time.sleep(delay)
        return result


def remove_stale_cache(cache_dir, zap_time):
    for p in cache_dir.glob('*'):
        try:
            t = int(p.name)
            if t >= zap_time:
                continue
        except:
            pass
        # print('Removing stale cache file:', p.name)
        p.unlink()


def tm_parse(tm):
    tm = tm.replace('Z', '+00:00')
    return datetime.datetime.fromisoformat(tm)


def sub_el(parent, name, text=None, **kwargs):
    el = ET.SubElement(parent, name, **kwargs)
    if text:
        el.text = text
    return el


def main():

    args = get_args()
    base_qs = {k[4:]: v for (k, v) in vars(args).items() if k.startswith('zap_')}

    print("Processing " + str(base_qs["postalCode"]))

    base_cache_dir = pathlib.Path(__file__).parent.joinpath('cache')
    if not base_cache_dir.is_dir():
        base_cache_dir.mkdir()

    cache_dir = pathlib.Path(base_cache_dir).joinpath(str(base_qs["postalCode"]))
    if not cache_dir.is_dir():
        cache_dir.mkdir()

    done_channels = False
    err = 0
    # Start time parameter is now rounded down to nearest `zap_timespan`, in s.
    zap_time = time.mktime(time.localtime())
    # print('Local time:    ', zap_time)
    zap_time_window = args.zap_timespan * 3600
    zap_time = int(zap_time - (zap_time % zap_time_window))
    # print('First zap time:', zap_time)

    remove_stale_cache(cache_dir, zap_time)

    out = ET.Element('tv')
    out.set('source-info-url', 'http://tvlistings.zap2it.com/')
    out.set('source-info-name', 'zap2it.com')
    out.set('generator-info-name', 'zap2xml.py')
    out.set('generator-info-url', 'github.com/deathbybandaid/zap2xml-py')

    # Fetch data in `zap_timespan` chunks.
    for i in range(int(7 * 24 / args.zap_timespan)):
        i_time = zap_time + (i * zap_time_window)
        # print('Getting data for', i_dt.isoformat())

        # build parameters for grid call
        parameters = {
                'aid': base_qs['aid'],
                'country': base_qs['country'],
                'device': base_qs['device'],
                'headendId': base_qs['headendId'],
                'isOverride': "true",
                'languagecode': base_qs['languagecode'],
                'pref': 'm,p',
                'timespan': base_qs['timespan'],
                'timezone': base_qs['timezone'],
                'userId': base_qs['userId'],
                'postalCode': base_qs['postalCode'],
                'lineupId': '%s-%s-DEFAULT' % (base_qs['country'], base_qs['device']),
                'time': i_time,
                'Activity_ID': 1,
                'FromPage': "TV%20Guide",
                }

    url = 'https://tvlistings.zap2it.com/api/grid?'
    url += urllib.parse.urlencode(parameters)

    result = get_cached(cache_dir, str(i_time), args.delay, url)
    d = json.loads(result)

    if not done_channels:
        done_channels = True
        for c_in in d['channels']:
            c_out = sub_el(out, 'channel',
                                id='I%s.%s.zap2it.com' % (c_in['channelNo'], c_in['channelId']))
            sub_el(c_out, 'display-name',
                          text='%s %s' % (c_in['channelNo'], c_in['callSign']))
            sub_el(c_out, 'display-name', text=c_in['channelNo'])
            sub_el(c_out, 'display-name', text=c_in['callSign'])
            channel_thumb = str(c_in['thumbnail']).replace("//", "").split("?")[0]
            sub_el(c_out, 'icon', src=channel_thumb)

    for c in d['channels']:
        c_id = 'I%s.%s.zap2it.com' % (c['channelNo'], c['channelId'])
        for event in c['events']:
            prog_in = event['program']
            tm_start = tm_parse(event['startTime'])
            tm_end = tm_parse(event['endTime'])
            prog_out = sub_el(out, 'programme',
                              start=tm_start.strftime('%Y%m%d%H%M%S %z'),
                              stop=tm_end.strftime('%Y%m%d%H%M%S %z'),
                              channel=c_id)

        if prog_in['title']:
            sub_el(prog_out, 'title', lang='en', text=prog_in['title'])

        if 'filter-movie' in event['filter'] and prog_in['releaseYear']:
            sub_el(prog_out, 'sub-title', lang='en', text='Movie: ' + prog_in['releaseYear'])
        elif prog_in['episodeTitle']:
            sub_el(prog_out, 'sub-title', lang='en', text=prog_in['episodeTitle'])

        if prog_in['shortDesc'] is None:
            prog_in['shortDesc'] = "Unavailable"
        sub_el(prog_out, 'desc', lang='en', text=prog_in['shortDesc'])

        sub_el(prog_out, 'length', units='minutes', text=event['duration'])

        for f in event['filter']:
            sub_el(prog_out, 'category', lang='en', text=f.replace('filter-', ''))
            sub_el(prog_out, 'genre', lang='en', text=f[7:])

        if event["thumbnail"] is not None:
            content_thumb = str("https://zap2it.tmsimg.com/assets/" + str(event['thumbnail']) + ".jpg")
            sub_el(prog_out, 'icon', src=content_thumb)

        if event['rating']:
            r = ET.SubElement(prog_out, 'rating')
            sub_el(r, 'value', text=event['rating'])

        if prog_in['season'] and prog_in['episode']:
            s_ = int(prog_in['season'], 10)
            e_ = int(prog_in['episode'], 10)
            sub_el(prog_out, 'episode-num', system='common',
                   text='S%02dE%02d' % (s_, e_))
            sub_el(prog_out, 'episode-num', system='xmltv_ns',
                   text='%d.%d.' % (int(s_)-1, int(e_)-1))
            sub_el(prog_out, 'episode-num', system='SxxExx">S',
                   text='S%02dE%02d' % (s_, e_))

        if 'New' in event['flag'] and 'live' not in event['flag']:
            sub_el(prog_out, 'new')

    out_path = str(args.output_file)
    with open(out_path, 'wb') as f:
        f.write(b'<?xml version="1.0" encoding="UTF-8"?>\n')
        f.write(ET.tostring(out, encoding='UTF-8'))

    sys.exit(err)


if __name__ == '__main__':
    main()
