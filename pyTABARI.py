import csv
import argparse
import datetime
import preprocess
import nltk as nk
from path import path
from joblib import Parallel, delayed


def sent_split(line):
    """
    Function to split a line into the important parts. Assumes the line is
    in the following format:

    event_id \t date \t sentences

    The date should be in ISO8601 (https://en.wikipedia.org/wiki/ISO_8601)
    format, which is YYYY-MM-DD.

    Inputs
    ------

    line : line containing the event id, date, and sentence in a tab-delimited
    format. String.

    Returns
    -------

    event_id : The id of the event. String.

    date : List of the component elements of the date in the format:
    ['YYYY', 'MM', 'DD'].

    tags : POS tagged sentence that can be passed to nltk.bigrams(), etc. List.

    """
    split = line.split('\t')
    event_id = split[0]
    date = split[1]
    date = date.split('-')
    sent = split[2]
    toks = nk.word_tokenize(sent)
    tags = nk.pos_tag(toks)
    return event_id, date, tags


def run_preprocess(tagged_sent, event_dict, event_id, username=None,
                   locate=True, features=True):
    """
    Function to call the various preprocessing functions. 

    Inputs
    ------
    tagged_sent : POS tagged sentence. List.

    event_dict : dictionary to which the event information should be added.

    event_id : ID for the event, which serves as the key in the dictionary.
    Should be generated from `sent_split`. String.

    username : geonames username. String.

    locate : Whether to geolocate an event. Boolean.

    features : Whether to extract features from an event. Boolean. 
    """
    #If geolocate
    if locate:
        #Locate event and add lat, lon to the dict
        lat, lon = preprocess.geolocate(tagged_sent, username)
        event_dict[event_id]['lat'] = lat
        event_dict[event_id]['lon'] = lon
    #If not geolocate just add 'NA' to dict
    elif not locate:
        event_dict[event_id]['lat'] = 'NA'
        event_dict[event_id]['lon'] = 'NA'
    #If feature extraction
    if features:
        #Get the number involved and assign to the dict
        num = preprocess.num_involved(tagged_sent)
        event_dict[event_id]['number_involved'] = num
    #If not just assign 'NA'
    elif not features:
        event_dict[event_id]['number_involved'] = 'NA'


def main(filepath=None, sent=None, username=None, locate=True, features=True):
    """
    Function that serves as the framework for other functions such as
    preprocessing and the parser itself.

    Inputs
    ------
    filepath : path to a file that contains sentences to parse. String.

    sent : If sentences are being passed, the sentence. String.

    locate : whether or not to geolocate the events. Boolean. Defaults to True.

    features : whether or not to extract features for the events. Boolean.
    Defaults to True.

    Returns
    -------
    events : dictionary of events with event_id as key and a dict of event
    info as the value.

    """
    events = dict()
    if sent:
        #Getting attributes and assigning them to the dict
        event_id, date, tags = sent_split(sent)
        events[event_id] = dict()
        events[event_id]['year'] = date[0]
        events[event_id]['month'] = date[1]
        events[event_id]['day'] = date[2]
        run_preprocess(tags, events, event_id, username=username,
                       locate=locate, features=features)
#    TODO: Actually add the parser.
#    src, target, cameo, goldstein = parser.parse()
#    events[event_id]['source'] = src
#    events[event_id]['target'] = target
#    events[event_id]['cameo'] = cameo
#    events[event_id]['goldstein'] = goldstein
        return events
    elif filepath:
        #If the input is a file of sentences, split the lines
        text = open(filepath, 'r').read()
        sents = text.split('\n')
        #Get the attributes. Will be a list of the format:
        #[('event_id', ['YYYY', 'MM', 'DD'], [POS tag sent]), (etc.)]
        sents = [sent_split(line) for line in sents if line]
        #Following is the same logic as above
        for sent in sents:
            events[sent[0]] = dict()
            events[sent[0]]['year'] = sent[1][0]
            events[sent[0]]['month'] = sent[1][1]
            events[sent[0]]['day'] = sent[1][2]
            run_preprocess(sent[2], events, sent[0], username=username,
                           locate=locate, features=features)
#    TODO: Actually add the parser.
#    src, target, cameo, goldstein = parser.parse(sent)
#    events[event_id]['source'] = src
#    events[event_id]['target'] = target
#    events[event_id]['cameo'] = cameo
#    events[event_id]['goldstein'] = goldstein
        return events


if __name__ == '__main__':
    print 'Running...'
    #Defining command-line arguments
    aparse = argparse.ArgumentParser()
    aparse.add_argument('-d', '--directory',
                        help='directory of files to parse', default=None)
    aparse.add_argument('-o', '--output', help='file to write events',
                        default=None)
    aparse.add_argument('-u', '--username', help="geonames username",
                        default=None)
    aparse.add_argument('-g', '--geolocate', action='store_true',
                        default=False, help="""Whether to geolocate events.
                        Defaults to False""")
    aparse.add_argument('-f', '--features', action='store_true', default=False,
                        help="""Whether to extract features from sentence.
                        Defaults to False""")
    aparse.add_argument('-n', '--n_cores', type=int, default=-1,
                        help="""Number of cores to use for parallel processing.
                        Defaults to -1 for all cores""")
    args = aparse.parse_args()
    in_path = args.directory
    out_path = args.output
    username = args.username

    #Getting files to parse. Different logic for path or single file.
    if not in_path:
        filepaths = path.getcwd().files('*.input.txt')
    elif path(in_path).isfile():
        filepaths = [in_path]
    elif path(in_path).isdir():
        filepaths = path(in_path).files('*.input.txt')

    #Calling main() in parallel using args.n_cores. Returns list of dicts.
    finalEvents = dict()
    #If more than 1 file, fork the files out in parallel
    if len(filepaths) > 1:
        print '%d files. Parsing files in parallel...' % len(filepaths)
        output = Parallel(n_jobs=args.n_cores)(delayed(main)
                         (filepath=filepath, locate=args.geolocate,
                          features=args.features, username=username) for
            filepath in filepaths)
    #If only one file, send the sentences out in parallel
    elif len(filepaths) == 1:
        print 'Single file. Parsing sentences in parallel...'
        text = open(filepaths[0], 'r').read()
        sents = text.split('\n')
        sents = [x for x in sents if x]
        output = Parallel(
            n_jobs=args.n_cores)(delayed(main)(
                sent=sent, locate=args.geolocate, features=args.features,
                username=username) for sent in sents)
    #Adding the dicts in output to one dict
    print 'Sentences parsed...'
    for events in output:
        finalEvents.update(events)

    #Creating filename for writing the file
    if not out_path:
        date = datetime.datetime.now()
        outFile = (
            path.getcwd() + '/events_' + str(date.year) + '.'
            + str(date.month) + '.' + str(date.day) + '.csv')
    elif out_path:
        outFile = out_path

    print 'Writing output...'
    f = csv.writer(open(outFile, "wb+"))
    header = ['event_id', 'year', 'month', 'day',
              'number_involved', 'lat', 'lon']
    f.writerow(header)

    for event in finalEvents:
        row = [str(event),
               finalEvents[event]['year'],
               finalEvents[event]['month'],
               finalEvents[event]['day'],
               finalEvents[event]['number_involved'],
               finalEvents[event]['lat'],
               finalEvents[event]['lon']]
        f.writerow(row)
    print 'Finished!'
