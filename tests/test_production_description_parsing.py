import unittest
import workflow_db as db

class ProductionDescriptionParsingTests(unittest.TestCase):
    def test_ewo26_00124_thread_box_and_pin(self):
        for end,letter in [('BOX','B'),('PIN','P')]:
            text=f'THREAD {end} END CONNECTION FOR 2 7/8" 7.90# P-110 R2 LONG-UPSET BEN-HT6 PER BENOIT PRINT # HT62875{letter} REV # 3.'
            parts=db._extract_inspection_entry_parts(text)
            self.assertEqual(parts,{'size':'2 7/8"','weight':'7.90#','connection':'BEN-HT6','end_type':end,'drawing':f'HT62875{letter} REV # 3.'})

    def test_ewo26_00124_turn_and_bore(self):
        for end in ['BOX','PIN']:
            text=f'TURN & BORE {end} END OF 2 7/8" 7.90# P-110 RANGE LONG-UPSET TUBING BEN-HT6 PER BENOIT PRINT # HT6TB REV # 0.'
            parts=db._extract_inspection_entry_parts(text)
            self.assertEqual((parts['size'],parts['weight'],parts['connection'],parts['end_type']),('2 7/8"','7.90#','BEN-HT6',end))

    def test_ben_hsf_ii_preserves_roman_numeral(self):
        text='THREAD BOX END CONNECTION FOR 2 3/8" 4.70# L-80 BEN-HSF II PER BENOIT PRINT # HSF2375B'
        self.assertEqual(db._extract_inspection_entry_parts(text)['connection'],'BEN-HSF II')

    def test_other_current_connection_formats(self):
        for text,connection in [
            ('THREAD BOX CONNECTION OF 3 1/2" 9.20# P-110 CY R2 NON-UPSET BEN-FJ PER BENOIT PRINT # FJ3500920B REV # 5.','BEN-FJ'),
            ('THREAD PIN END CONNECTION FOR 2 3/8" 4.70# L-80 3% CR R2 IJ-UPSET BEN-EUI PER BENOIT PRINT # EUI2375470P REV # 6.','BEN-EUI')]:
            self.assertEqual(db._extract_inspection_entry_parts(text)['connection'],connection)

    def test_ewo26_00129_separates_upset_from_thread_connection(self):
        for end,revision in [('BOX','10'),('PIN','6')]:
            descriptions=[
                f'THREAD {end} END CONNECTION FOR 2 3/8" 4.70# L-80 3% CR R2 BEN-MU UPSET BEN-EUI PER BENOIT PRINT # EUI2375470{end[0]} REV # {revision}.',
                f'TURN & BORE {end} END OF 2 3/8" 4.70# L-80 3% RANGE 2 BEN-MU UPSET TUBING BEN-EUI PER BENOIT PRINT # BENMUTB REV # 2.',
            ]
            for text in descriptions:
                with self.subTest(text=text):
                    parts=db._extract_inspection_entry_parts(text)
                    self.assertEqual((parts['size'],parts['weight'],parts['connection'],parts['end_type']),('2 3/8"','4.70#','BEN-EUI',end))

    def test_upset_only_and_multiple_real_connections_stay_unresolved(self):
        for suffix in ['BEN-MU UPSET TUBING','BEN-FJ OR BEN-EUI','BEN-MU UPSET BEN-FJ OR BEN-EUI']:
            with self.subTest(suffix=suffix):
                text=f'THREAD BOX END CONNECTION FOR 2 3/8" 4.70# L-80 {suffix} PER BENOIT PRINT # EXAMPLE'
                self.assertIsNone(db._extract_inspection_entry_parts(text)['connection'])

    def test_no_invented_connection_for_grade_upset_or_inspection_only(self):
        self.assertIsNone(db._extract_inspection_entry_parts('TURN & BORE BOX END OF 2 3/8" 4.70# L-80 3% RANGE 2 IJ-UPSET TUBING')['connection'])
        self.assertIsNone(db._extract_inspection_entry_parts('INSPECT BOX END CONNECTION')['size'])
        ambiguous='THREAD BOX END 2 7/8" 7.90# OR 3 1/2" 9.20# BEN-HT6'
        self.assertIsNone(db._extract_inspection_entry_parts(ambiguous)['size'])

    def test_labelled_end_without_on_and_drawing_boundary(self):
        text='Thread Pin ends with Size: 4.500, Weight:11.60#, Connection: BEN-TCX - Drawing# TCX4500P REV.2'
        parts=db._extract_inspection_entry_parts(text)
        self.assertEqual(parts['end_type'],'PIN')
        self.assertEqual(parts['connection'],'BEN-TCX')

if __name__=='__main__':unittest.main()
