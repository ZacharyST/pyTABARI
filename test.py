import preprocess
import unittest
import pyTABARI


class TestSequenceFunctions(unittest.TestCase):

    def setUp(self):
        text = open('test_files/test.txt', 'r').read()
        sents = text.split('\n')
        sents = [pyTABARI.sent_split(line) for line in sents if line]
        self.tag_sentence = sents

    def test_sent_split(self):
        reference = [('0', ['2012', '02', '15'], [('Syrian', 'JJ'),
                     ('rebels', 'NNS'), ('killed', 'VBD'), ('28', 'CD'),
                     ('soldiers', 'NNS'), ('in', 'IN'), ('attacks', 'NNS'),
                     ('on', 'IN'), ('three', 'CD'), ('army', 'NN'),
                     ('checkpoints', 'NNS'), ('on', 'IN'), ('the', 'DT'),
                     ('main', 'JJ'), ('road', 'NN'), ('from', 'IN'),
                     ('Damascus', 'NNP'), ('to', 'TO'), ('the', 'DT'),
                     ('embattled', 'JJ'), ('city', 'NN'), ('of', 'IN'),
                     ('Aleppo', 'NNP'), ('Thursday', 'NNP'), (',', ','),
                     ('a', 'DT'), ('watchdog', 'NN'), ('said', 'VBD'),
                     ('.', '.')]), ('1', ['2012', '02', '15'],
                    [('Gunmen', 'NNS'), (',', ','), ('numbering', 'VBG'),
                     ('about', 'IN'), ('20', 'CD'), (',', ','),
                     ('have', 'VBP'), ('attacked', 'VBN'), ('Kaboro', 'NNP'),
                     ('community', 'NN'), ('in', 'IN'), ('the', 'DT'),
                     ('Dansadau', 'NNP'), ('Emirate', 'NNP'), ('of', 'IN'),
                     ('Zamfara', 'NNP'), ('State', 'NNP'), (',', ','),
                     ('killing', 'VBG'), ('18', 'CD'), ('people', 'NNS'),
                     (',', ','), ('including', 'VBG'), ('the', 'DT'),
                     ('village', 'NN'), ('head', 'NN'), ('.', '.')]),
                     ('2', ['2012', '02', '16'], [('Rebels', 'NNS'),
                     ('killed', 'VBD'), ('28', 'CD'), ('soldiers', 'NNS'),
                     ('in', 'IN'), ('Syria', 'NNP'), ("'s", 'POS'),
                     ('northwestern', 'NN'), ('battlefields', 'NNS'),
                     ('Thursday', 'NNP'), (',', ','), ('a', 'DT'),
                     ('watchdog', 'NN'), ('said', 'VBD'), (',', ','),
                     ('as', 'IN'), ('the', 'DT'), ('regime', 'NN'),
                     ('launched', 'VBD'), ('new', 'JJ'), ('air', 'NN'),
                     ('strikes', 'NNS'), ('in', 'IN'), ('what', 'WP'),
                     ('is', 'VBZ'), ('seen', 'VBN'), ('as', 'IN'),
                     ('a', 'DT'), ('desperate', 'NN'), ('attempt', 'NN'),
                     ('to', 'TO'), ('reverse', 'VB'), ('opposition', 'NN'),
                     ('gains', 'NNS'), ('.', '.')]),
                     ('3', ['2012', '02', '17'], [('Six', 'CD'),
                     ('killed', 'VBD'), ('in', 'IN'), ('an', 'DT'),
                     ('attack', 'NN'), ('on', 'IN'), ('ANP', 'NNP'),
                     ('office', 'NN'), ('and', 'CC'), ('terrorism', 'NN'),
                     ('in', 'IN'), ('Karachi', 'NNP'), ('.', '.')])]
        text = open('test_files/test.txt', 'r').read()
        sents = text.split('\n')
        output = [pyTABARI.sent_split(line) for line in sents if line]
        self.assertEqual(reference, output)

    def test_geolocate(self):
        reference = [('33.5101981468', '36.2912750244'),
                     ('12.1666667', '6.25'),
                     ('35', '38'), ('24.9056', '67.0822')]
        output = [preprocess.geolocate(x[2], '######') for x in
                  self.tag_sentence]
        self.assertEqual(reference, output)

    def test_number_involved(self):
        reference = ['28', '18', '28', '6']
        output = [preprocess.num_involved(x[2]) for x in self.tag_sentence]
        self.assertEqual(reference, output)

    def test_main(self):
        reference = {'0': {'day': '15', 'lat': '33.5101981468',
                     'lon': '36.2912750244',
                     'month': '02', 'number_involved': '28', 'year': '2012'},
                     '1': {'day': '15', 'lat': '12.1666667', 'lon': '6.25',
                     'month': '02', 'number_involved': '18', 'year': '2012'},
                     '2': {'day': '16', 'lat': '35', 'lon': '38',
                     'month': '02', 'number_involved': '28', 'year': '2012'},
                     '3': {'day': '17', 'lat': '24.9056', 'lon': '67.0822',
                     'month': '02', 'number_involved': '6', 'year': '2012'}}
        output = pyTABARI.main('test_files/test.txt', username='######')
        self.assertEqual(reference, output)


if __name__ == '__main__':
    unittest.main()
