from zipfile import ZipFile
from sys import argv
from pprint import pprint
import os
import subprocess
import json
import datetime
import requests

from date_helper import parse_hytek_date, to_sql_date, get_hytek_dob

class HytekDbImporter:

    def __init__(self, db_file):
        self.db_file = db_file
        self.hytek_events_db = []
        self.hytek_teams_db = []
        self.hytek_athletes_db = []
        self.hytek_relay_teams_db = []
        self.openmeet_meet = {}
        self.openmeet_teams_db = []
        self.openmeet_athletes_db = []
        self.openmeet_events = []
        self.openmeet_entries_db = []

    def find_hytek_team(self, team_no):
        for team in self.hytek_teams_db:
            if team['Team_no'] == team_no:
                return team
        return None

    def find_hytek_athlete(self, ath_no):
        for athlete in self.hytek_athletes_db:
            if athlete['Ath_no'] == ath_no:
                return athlete
        return None

    def find_hytek_event(self, event_ptr):
        for event in self.hytek_events_db:
            if event['Event_no'] == event_ptr:
                return event
        return None

    def find_openmeet_athlete(self, reg_no):
        athlete = None
        for athlete_search in self.openmeet_athletes_db:
            if athlete_search['member_number'] == reg_no:
                athlete = athlete_search
        return athlete

    def find_openmeet_entry(self, program_number, athlete_id):
        for entry in self.openmeet_entries_db:
            if entry['program_number'] == str(program_number) and entry['athlete_id'] == athlete_id:
                return entry
        return None

    def get_meet_setup(self):
        meet_data = subprocess.run(['mdb-json', self.db_file, 'meet'], stdout=subprocess.PIPE)
        meet_json = json.loads(meet_data.stdout)

        meet_create = {
            'meetname': meet_json['Meet_name1'],
            'startdate': to_sql_date(parse_hytek_date(meet_json['Meet_start'])),
            'enddate': to_sql_date(parse_hytek_date(meet_json['Meet_end'])),
            'deadline': parse_hytek_date(meet_json['entry_deadline']).isoformat(),
            'max_individual_events': meet_json['indmax_perath'],
            'max_relay_events': meet_json['relmax_perath'],
            'max_total_events': meet_json['entrymax_total'],
            'age_up_date': to_sql_date(parse_hytek_date(meet_json['Calc_date'])),
        }

        event_data = subprocess.run(['mdb-json', self.db_file, 'event'], stdout=subprocess.PIPE, text=True).stdout.splitlines()

        events = []

        for event_row in event_data:
            event_json = json.loads(event_row)
            self.hytek_events_db.append(event_json)
            legs = 1

            # pprint(event_json)
            event_type = ''     # TODO: handle other event types
            if meet_json['Meet_class'] == 6 and event_json['Event_rounds'] == 1:
                # this is a masters meet timed final event

                if event_json['Ind_rel'] == 'I':
                    # Individual event
                    event_type = 'Seeded Individual Mixed Finals'

                if event_json['Ind_rel'] == 'R':
                    # Relay event
                    if event_json['Event_gender'] == 'M':
                        event_type = 'Seeded Mens Relay Finals'
                    if event_json['Event_gender'] == 'F':
                        event_type = 'Seeded Womens Relay Finals'
                    if event_json['Event_gender'] == 'X':
                        event_type = 'Seeded Mixed Relay Finals'

                    legs = event_json['Num_RelayLegs']

            event_discipline = ''
            if event_json['Event_stroke'] == 'A':
                event_discipline = 'Freestyle'
            if event_json['Event_stroke'] == 'B':
                event_discipline = 'Backstroke'
            if event_json['Event_stroke'] == 'C':
                event_discipline = 'Breaststroke'
            if event_json['Event_stroke'] == 'D':
                event_discipline = 'Butterfly'
            if event_json['Event_stroke'] == 'E':
                event_discipline = 'Individual Medley'  # TODO: handle relays

            meet_course = ''
            if meet_json['Meet_course'] == 1:
                meet_course = "LC"
            if meet_json['Meet_course'] == 2:
                meet_course = "SC"

            distance = str(event_json['Event_dist']) + 'm ' + meet_course

            event = {
                'event_type': event_type,
                'event_order': int(event_json['Event_no']),
                'program_number': event_json['Event_no'],
                'discipline': event_discipline,
                'distance': distance,
                'legs': legs
            }

            events.append(event)

        meet_create['events'] = events

        return meet_create


    def get_teams(self):

        team_data = subprocess.run(['mdb-json', self.db_file, 'team'], stdout=subprocess.PIPE, text=True).stdout.splitlines()

        teams = []

        for team_row in team_data:
            team_json = json.loads(team_row)
            self.hytek_teams_db.append(team_json)

            team = {
                'team_id': team_json['Team_no'],
                'team_name': team_json['Team_name'].rstrip(),
                'abbreviation': team_json['Team_abbr'].rstrip(),
                'members': []
            }

            teams.append(team)

        return teams


    def get_athletes(self, teams):
        athlete_data = subprocess.run(['mdb-json', self.db_file, 'athlete'], stdout=subprocess.PIPE, text=True).stdout.splitlines()

        for athlete_row in athlete_data:
            athlete_json = json.loads(athlete_row)
            self.hytek_athletes_db.append(athlete_json)

            team = next((x for x in teams if x['team_id'] == athlete_json['Team_no']), None)

            athlete = {
                'athlete_id': athlete_json['Comp_no'],
                'surname': athlete_json['Last_name'].rstrip(),
                'first_name': athlete_json['First_name'].rstrip(),
                'other_names': athlete_json['Initial'].rstrip(),
                'preferred_name': athlete_json['Pref_name'].rstrip(),
                'sex': athlete_json['Ath_Sex'].rstrip(),
                'dob': to_sql_date(get_hytek_dob(athlete_json['Birth_date'], athlete_json['Ath_age'])),
                'age': athlete_json['Ath_age'],
                'member_number': athlete_json['Reg_no'].rstrip(),
                'team_id': athlete_json['Team_no']
            }

            team['members'].append(athlete)

    def get_entries(self):
        entry_data = subprocess.run(['mdb-json', self.db_file, 'entry'], stdout=subprocess.PIPE, text=True).stdout.splitlines()

        entries = []

        for entry_row in entry_data:
            entry_json = json.loads(entry_row)

            # Find Team and Athlete information
            hytek_athlete = self.find_hytek_athlete(entry_json['Ath_no'])
            hytek_team = self.find_hytek_team(hytek_athlete['Team_no'])

            # Find OpenMeet team and athlete
            team = next((x for x in self.openmeet_teams_db if x['abbreviation'] == hytek_team['Team_abbr'].strip()), None)
            athlete = next((x for x in team['members'] if x['member_number'] == hytek_athlete['Reg_no'].strip()), None)

            meet_event = self.find_hytek_event(entry_json['Event_ptr'])
            # TODO: Handle event letter

            if meet_event is None:
                print('Error unable to find event %s' % entry_json['Event_ptr'])
                # TODO: Raise exception

            existing_entry = self.find_openmeet_entry(meet_event['Event_no'], athlete['athlete_id'])

            if existing_entry is not None:
                # print("Found existing entry for %s, %s(%s) in event %s" % (athlete['surname'],
                #                                                            athlete['first_name'],
                #                                                            team['abbreviation'],
                #                                                            meet_event['Event_no']))

                # TODO: check for changes and update if necessary

                continue

            # print("Create entry for %s, %s(%s) in event %s" % (athlete['surname'],
            #                                                    athlete['first_name'],
            #                                                    team['abbreviation'],
            #                                                    meet_event['Event_no']))

            seed_time = None
            if 'ConvSeed_time' in entry_json:
                seed_time = entry_json['ConvSeed_time']
            elif 'ActualSeed_time' in entry_json:
                seed_time = entry_json['ActualSeed_time']

            entry = {
                'athlete_id': athlete['athlete_id'],
                'meet_id': self.openmeet_meet['meet_id'],
                'team_id': team['team_id'],
                'program_number': meet_event['Event_no'],
                'seed_time': seed_time,
                'status_code': 'ENTERED',
                'scratched': entry_json['Scr_stat']
            }

            entries.append(entry)

        return entries


    def get_relay_teams(self):
        relay_teams = subprocess.run(['mdb-json', self.db_file, 'relay'], stdout=subprocess.PIPE,
                                    text=True).stdout.splitlines()
        relay_names = subprocess.run(['mdb-json', self.db_file, 'relaynames'], stdout=subprocess.PIPE,
                                     text=True).stdout.splitlines()

        relay_teams_new = []

        for relay_team in relay_teams:
            relay_team_json = json.loads(relay_team)

            # Find OpenMeet team and athlete
            hytek_team = self.find_hytek_team(relay_team_json['Team_no'])
            openmeet_team = next((x for x in self.openmeet_teams_db if x['abbreviation'] == hytek_team['Team_abbr'].strip()), None)
            hytek_event = self.find_hytek_event(relay_team_json['Event_ptr'])
            # TODO: Handle event letter

            # Get names for this relay
            relay_members_new = []
            for relay_name in relay_names:
                relay_name_json = json.loads(relay_name)
                if relay_name_json['Relay_no'] == relay_team_json['Relay_no']:
                    hytek_athlete = self.find_hytek_athlete(relay_name_json['Ath_no'])

                    openmeet_athlete = next((x for x in self.openmeet_athletes_db if x['member_number'] == hytek_athlete['Reg_no'].strip()), None)

                    relay_member_new = {
                        'leg': relay_name_json['Pos_no'],
                        'athlete_id': openmeet_athlete['athlete_id']
                    }

                    relay_members_new.append(relay_member_new)

            relay_team_new = {
                'meet_id': self.openmeet_meet['meet_id'],
                'program_number': hytek_event['Event_no'],
                'team_id': openmeet_team['team_id'],
                'seed_time': relay_team_json['ConvSeed_time'],
                'letter': relay_team_json['Team_ltr'],
                'scratched':  relay_team_json['Scr_stat'],
                'status_code': 'ENTERED',
                'members': relay_members_new
            }

            relay_teams_new.append(relay_team_new)

        return relay_teams_new


    def get_individual_results(self):
        entry_data = subprocess.run(['mdb-json', self.db_file, 'entry'], stdout=subprocess.PIPE, text=True).stdout.splitlines()

        results = []

        for entry_row in entry_data:
            entry_json = json.loads(entry_row)

            # Find Team and Athlete information
            hytek_athlete = self.find_hytek_athlete(entry_json['Ath_no'])
            hytek_team = self.find_hytek_team(hytek_athlete['Team_no'])

            # Find OpenMeet team and athlete
            team = next((x for x in self.openmeet_teams_db if x['abbreviation'] == hytek_team['Team_abbr'].strip()), None)
            athlete = next((x for x in team['members'] if x['member_number'] == hytek_athlete['Reg_no'].strip()), None)

            meet_event = self.find_hytek_event(entry_json['Event_ptr'])
            # TODO: Handle event letter

            if meet_event is None:
                print('Error unable to find event %s' % entry_json['Event_ptr'])
                # TODO: Raise exception

            if 'Fin_Time' in entry_json:
                final_time = entry_json['Fin_Time']
            else:
                final_time = None

            if 'Fin_pad' in entry_json:
                pad_time = entry_json['Fin_pad']
            else:
                pad_time = None

            if 'Fin_back1' in entry_json:
                backup1_time = entry_json['Fin_back1']
            else:
                backup1_time = None

            if 'Fin_back2' in entry_json:
                backup2_time = entry_json['Fin_back2']
            else:
                backup2_time = None

            if 'Fin_back3' in entry_json:
                backup3_time = entry_json['Fin_back3']
            else:
                backup3_time = None

            openmeet_entry = self.find_openmeet_entry(meet_event['Event_no'], athlete['athlete_id'])

            # Nullify any 0 times
            final_time_result = None
            if final_time is not None and final_time != 0:
                final_time_result = {
                    'entry_id': openmeet_entry['entry_id'],
                    'meet_id': self.openmeet_meet['meet_id'],
                    'seconds': final_time
                }

            heat_time_results = []

            if pad_time is not None and pad_time != 0:
                heat_time_results.append({
                    'entry_id': openmeet_entry['entry_id'],
                    'meet_id': self.openmeet_meet['meet_id'],
                    'seconds': pad_time,
                    'time_type_code': 'PAD',
                })

            if backup1_time is not None and backup1_time != 0:
                heat_time_results.append({
                    'entry_id': openmeet_entry['entry_id'],
                    'meet_id': self.openmeet_meet['meet_id'],
                    'seconds': backup1_time,
                    'time_type_code': 'BACKUP1',
                })

            if backup2_time is not None and backup2_time != 0:
                heat_time_results.append({
                    'entry_id': openmeet_entry['entry_id'],
                    'meet_id': self.openmeet_meet['meet_id'],
                    'seconds': backup2_time,
                    'time_type_code': 'BACKUP2',
                })

            if backup3_time is not None and backup3_time != 0:
                heat_time_results.append({
                    'entry_id': openmeet_entry['entry_id'],
                    'meet_id': self.openmeet_meet['meet_id'],
                    'seconds': backup3_time,
                    'time_type_code': 'BACKUP3',
                })

            entry_results = {
                'entry_id': openmeet_entry['entry_id'],
                'meet_id': self.openmeet_meet['meet_id'],
                'final_result': final_time_result,
                'heat_results': heat_time_results
            }

            # Don't add full null results
            if final_time_result is None and len(heat_time_results) == 0:
                continue

            # if final_time_result is None:
            #     print('final time result is none')
            #     pprint(final_time_result)

            results.append(entry_results)

        return results


    def get_existing_entries(self, meet_id):
        response = requests.get("http://localhost:8000/meet/%d/entries" % meet_id)

        if response.status_code == 200:
            response_json = response.json()
            self.openmeet_entries_db = response_json['data']
            return True
        else:
            print('Error retrieving existing entries')
            print(response.status_code)
            print(response.text)
            exit()


    def open_hytek_db(self):

        # Get meet setup data
        meet_create = self.get_meet_setup()

        # Check if meet already exists
        meet_response = requests.get('http://localhost:8000/meet?meetname=' + meet_create['meetname'])

        if meet_response.status_code == 404:
            print("Meet doesn't exist")

            r = requests.post('http://localhost:8000/meet', data=json.dumps(meet_create))
            response_json = r.json()
            meet = response_json['data']

        elif meet_response.status_code== 200:
            print('Meet does exist')

            response_json = meet_response.json()
            meet = response_json['data']

        else:
            print('Error creating or retrieving meet')
            print(meet_response.status_code)
            print(meet_response.text)
            exit()

        # TODO: Update Meet Data
        self.openmeet_meet = meet
        self.openmeet_events = meet['events']

        # Load teams
        teams = self.get_teams()

        # Load athletes
        self.get_athletes(teams)

        teams_response = requests.get('http://localhost:8000/teams')
        response_json = teams_response.json()
        teams_data = response_json['data']

        teams_to_add = []
        members_to_add = []
        for team in teams:
            # Find this team in team database by abbreviation
            existing_team = next((x for x in teams_data if x['abbreviation'] == team['abbreviation']), None)

            if existing_team is None:
                existing_team = next((x for x in teams_data if x['team_name'] == team['team_name']), None)

                if existing_team is not None:
                    # TODO: report exception
                    print("Found existing team %s with different abbreviation: OpenMeet=%s Hytek MM=%s" % (existing_team['team_name'],
                                                                                                           existing_team['abbreviation'],
                                                                                                           team['abbreviation']))

            if existing_team is None:
                teams_to_add.append(team)
            else:
                # print("Found existing team %s(%s)" % (existing_team['team_name'], existing_team['abbreviation']))
                # Compare team members
                for member in team['members']:
                    existing_member = next((x for x in existing_team['members'] if x['member_number'] == member['member_number']), None)
                    if existing_member is None:
                        # print('Found existing athlete %s, %s(%s)' % (member['surname'],
                        #                                              member['first_name'],
                        #                                              member['member_number']))
                    # else:
                    #     print('Adding athlete %s, %s(%s)' % (member['surname'],
                    #                                          member['first_name'],
                    #                                          member['member_number']))
                        members_to_add.append(member)

        # Post teams to backend
        r = requests.post('http://localhost:8000/teams', data=json.dumps(teams_to_add))
        if r.status_code != 200:
            print('Error adding teams')
            print(r.status_code)
            print(r.text)
            exit()

        r = requests.post('http://localhost:8000/athletes', data=json.dumps(members_to_add))
        if r.status_code != 200:
            print('Error adding athletes')
            print(r.status_code)
            print(r.text)
            exit()
        else:
            # print('Successfully added athletes:')
            athletes_added = r.json()


        # Get DB of all teams and entrants
        teams_request = requests.get('http://localhost:8000/teams')
        teams_response = teams_request.json()
        self.openmeet_teams_db = teams_response['data']

        # Populate Openmeet Athlete DB
        for team in self.openmeet_teams_db:
            for member in team['members']:
                self.openmeet_athletes_db.append(member)

        # Load entries
        self.get_existing_entries(self.openmeet_meet['meet_id'])
        entries = self.get_entries()

        entries_request = requests.post("http://localhost:8000/meet/%s/entries" % self.openmeet_meet['meet_id'],
                                        data=json.dumps(entries))
        entries_response = entries_request.json()
        self.openmeet_entries_db.append(entries_response['data'])
        # pprint(self.openmeet_entries_db)

        # Load individual results
        individual_results = self.get_individual_results()
        # pprint(individual_results)

        results_request = requests.put("http://localhost:8000/meet/%s/results" % self.openmeet_meet['meet_id'],
                                        data=json.dumps(individual_results))
        results_response = results_request.json()
        pprint(results_response)

        # Get DB of all relay teams and members
        relay_teams = self.get_relay_teams()

        r = requests.post("http://localhost:8000/meet/%s/relays" % self.openmeet_meet['meet_id'],
                          data=json.dumps(relay_teams))
        relay_teams_response = r.json()


if __name__ == '__main__':

    if len(argv) > 1:
        input_file = argv[1]
        data_file = ""

        if input_file.split('.')[-1] == 'zip':
            with ZipFile(input_file) as myzip:
                for zip_info in myzip.infolist():
                    if zip_info.filename.split('.')[-1] == 'mdb':
                        data_file = os.path.basename(zip_info.filename)
                        zip_info.filename = os.path.basename(zip_info.filename)
                        myzip.extract(zip_info, os.getcwd())
                        break

        if input_file.split('.')[-1] == 'mdb':
            data_file = input_file

        if data_file != "":
            print('Loading %s' % data_file)
            importer = HytekDbImporter(data_file)
            importer.open_hytek_db()

    else:
        print("No Hy-Tek Meet Manager database specified!")
