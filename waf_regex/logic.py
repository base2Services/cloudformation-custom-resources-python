import boto3
import os
import glob
import logging
import pprint


logger = logging.getLogger()
logger.setLevel(logging.INFO)


class WafRegexLogic:

    def __init__(self, resource_properties):
        self.regex_patterns = resource_properties['RegexPatterns']
        self.match_type = resource_properties['Type']
        self.match_data = resource_properties['Data']
        self.transform = resource_properties['Transform']
        self.match_name = resource_properties['Name']
        self.pattern_name = f"{resource_properties['Name']}-pattern"
        self.client = boto3.client('waf-regional')

    def new_pattern_set(self):
        changeToken = self.client.get_change_token()
        response_create_pattern_set = self.client.create_regex_pattern_set(
            Name=self.pattern_name,
            ChangeToken=changeToken['ChangeToken']
        )
        for pattern in self.regex_patterns:
            self.insert_pattern_set(response_create_pattern_set['RegexPatternSet']['RegexPatternSetId'], pattern)
        return response_create_pattern_set['RegexPatternSet']['RegexPatternSetId']

    def remove_pattern_set(self,pattern_set_id):
        pattern_set_object = self.get_pattern_set(pattern_set_id)
        for pattern_set_string in pattern_set_object['RegexPatternSet']['RegexPatternStrings']:
            self.delete_pattern_set(pattern_set_id, pattern_set_string)
        changeToken = self.client.get_change_token()
        response = self.client.delete_regex_pattern_set(
            RegexPatternSetId=pattern_set_id,
            ChangeToken=changeToken['ChangeToken']
        )

    def get_pattern_set(self,pattern_set_id):
        pattern_set_object = self.client.get_regex_pattern_set(
            RegexPatternSetId=pattern_set_id
        )
        return pattern_set_object


    def update_pattern_set(self,pattern_set_id):
        pattern_set_object = self.get_pattern_set(pattern_set_id)
        #delete existing and add a new one
        for pattern_set_string in pattern_set_object['RegexPatternSet']['RegexPatternStrings']:
            self.delete_pattern_set(pattern_set_id, pattern_set_string)
        for pattern in self.regex_patterns:
            self.insert_pattern_set(pattern_set_id, pattern)


    def insert_pattern_set(self,pattern_set_id, pattern_set_string):
        changeToken = self.client.get_change_token()
        update_regex_patternset = self.client.update_regex_pattern_set(
            RegexPatternSetId=pattern_set_id,
            Updates=[
                {
                    'Action': 'INSERT',
                    'RegexPatternString': pattern_set_string
                },
            ],
            ChangeToken=changeToken['ChangeToken']
        )

    def delete_pattern_set(self,pattern_set_id, pattern_set_string):
        changeToken = self.client.get_change_token()
        update_regex_patternset = self.client.update_regex_pattern_set(
            RegexPatternSetId=pattern_set_id,
            Updates=[
                {
                    'Action': 'DELETE',
                    'RegexPatternString': pattern_set_string
                },
            ],
            ChangeToken=changeToken['ChangeToken']
        )

    #
    # Match Sets
    #

    def new_match_set(self):
        changeToken = self.client.get_change_token()
        #create match set
        response_create_match_set = self.client.create_regex_match_set(
            Name=self.match_name,
            ChangeToken=changeToken['ChangeToken']
        )
        #create pattern set
        pattern_set_id = self.new_pattern_set()
        self.insert_match_set(response_create_match_set['RegexMatchSet']['RegexMatchSetId'], pattern_set_id)
        return response_create_match_set['RegexMatchSet']['RegexMatchSetId']

    def insert_match_set(self, match_set_id, pattern_set_id):
        changeToken = self.client.get_change_token()
        update_regex_matchset = self.client.update_regex_match_set(
            RegexMatchSetId=match_set_id,
            Updates=[
                {
                    'Action': 'INSERT',
                    'RegexMatchTuple': {
                        'FieldToMatch': {
                            'Type': self.match_type,
                            'Data': self.match_data
                        },
                        'TextTransformation': self.transform,
                        'RegexPatternSetId': pattern_set_id
                    }
                },
            ],
            ChangeToken=changeToken['ChangeToken']
        )

    def update_match_set(self,match_set_id):
        match_set_object = self.get_match_set(match_set_id)
        for match_tuple in match_set_object['RegexMatchSet']['RegexMatchTuples']:
            self.update_pattern_set(match_tuple['RegexPatternSetId'])
            self.delete_match_set(match_set_id,match_tuple)
            self.insert_match_set(match_set_id,match_tuple['RegexPatternSetId'])


    def get_match_set(self,match_set_id):
        match_set_object = self.client.get_regex_match_set(
            RegexMatchSetId=match_set_id
        )
        return match_set_object

    def remove_match_set(self,match_set_id):
        match_set_object = self.get_match_set(match_set_id)

        for match_tuple in match_set_object['RegexMatchSet']['RegexMatchTuples']:
            if bool(match_tuple):
                self.delete_match_set(match_set_id,match_tuple)
                self.remove_pattern_set(match_tuple['RegexPatternSetId'])

        changeToken = self.client.get_change_token()
        response = self.client.delete_regex_match_set(
            RegexMatchSetId=match_set_id,
            ChangeToken=changeToken['ChangeToken']
        )

    def delete_match_set(self,match_set_id,match_tuple):
        changeToken = self.client.get_change_token()
        update_regex_matchset = self.client.update_regex_match_set(
            RegexMatchSetId=match_set_id,
            Updates=[
                {
                    'Action': 'DELETE',
                    'RegexMatchTuple': match_tuple
                },
            ],
            ChangeToken=changeToken['ChangeToken']
        )
