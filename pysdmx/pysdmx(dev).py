#! /usr/bin/env python3
# -*- coding: utf-8 -*-
""" Python interface to SDMX """

import requests
import pandas
import lxml.etree
import uuid
import datetime
import numpy
import logging
import pudb #
import pdb #


def date_parser(date, frequency):
    if frequency == 'A':
        return datetime.datetime.strptime(date, '%Y')
    if frequency == 'Q':
        date = date.split('-Q')
        date = str(int(date[1])*3) + date[0]
        return datetime.datetime.strptime(date, '%m%Y')
    if frequency == 'M':
        return datetime.datetime.strptime(date, '%Y-%m')


def query_rest(url):
    logging.basicConfig( level=logging.DEBUG, format='%(asctime)s %(levelname)s  %(message)s')#
    #pudb.set_trace()
    request = requests.get(url, timeout= 20)
    if request.status_code != requests.codes.ok:
        raise ValueError("Error getting client({})".format(request.status_code))      
    parser = lxml.etree.XMLParser(
            ns_clean=True, recover=True, encoding='utf-8')
    return lxml.etree.fromstring(
            request.text.encode('utf-8'), parser=parser)


class Codelist(object): # done
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._codes = None

    @property
    def codes(self):
        if not self._codes:
            self._codes = {}
            codelists = self.tree.xpath(".//message:CodeLists",
                    namespaces=self.tree.nsmap)
            for codelists_ in codelists:
                for codelist in codelists_.iterfind(".//structure:CodeList",
                        namespaces=self.tree.nsmap):
                    name = codelist.xpath('.//structure:Name', namespaces=self.tree.nsmap)
                    name = name[0]
                    name = name.text
                    code = []
                    for code_ in codelist.iterfind(".//structure:Code",
                            namespaces=self.tree.nsmap):
                        code_key = code_.get('value')
                        code_name = code_.xpath('.//structure:Description',
                                namespaces=self.tree.nsmap)
                        code_name = code_name[0]
                        code.append((code_key,code_name.text))
                    self._codes[name] = code
        return self._codes


class Dataflows(object):  # ok
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._all_dataflows = None

    @property
    def all_dataflows(self):  
        #pudb.set_trace()
        if not self._all_dataflows:
            self._all_dataflows = {}
            for dataflow in self.tree.iterfind(".//structure:Dataflow",
                    namespaces=self.tree.nsmap):
                id = dataflow.get('id')
                agencyID = dataflow.get('agencyID')
                version = dataflow.get('version')
                #Keyfamilyref = {}
                name = dataflow.xpath('.//structure:Name', namespaces=self.tree.nsmap)[0].text
                for keyfamilyref in dataflow.iterfind(".//structure:KeyFamilyRef",
                        namespaces=self.tree.nsmap):
                    keyfamilyid = keyfamilyref.xpath('.//structure:KeyFamilyID', namespaces=self.tree.nsmap)[0].text
                    keyfamilyagenceid =  keyfamilyref.xpath('.//structure:KeyFamilyAgencyID', namespaces=self.tree.nsmap)[0].text
                categoryscheme = dataflow.xpath('.//structure:CategorySchemeID', namespaces=self.tree.nsmap)[0].text
                categoryID = dataflow.xpath('.//structure:ID', namespaces=self.tree.nsmap)[0].text
                self._all_dataflows[id] = (agencyID, version, name,
                        keyfamilyid,
                        keyfamilyagenceid,categoryscheme,categoryID ) 
                return self._all_dataflows


class Data(object):#ok
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._time_series = None

    @property
    def time_series(self):
        if not self._time_series:
            self._time_series = {}

            for group  in self.tree.iterfind(".//generic:Group",
                    namespaces=self.tree.nsmap):
                for series in group.iterfind(".//generic:Series",
                        namespaces=self.tree.nsmap):
                    codes = {}
                    for key in series.iterfind(".//generic:Value",
                            namespaces=self.tree.nsmap):
                        codes[key.get('concept')] = key.get('value')
                    time_series_ = []
                    for observation in series.iterfind(".//generic:Obs",
                            namespaces=self.tree.nsmap):
                        dimensions = observation.xpath(".//generic:Time",
                                namespaces=self.tree.nsmap)
                        dimension = dimensions[0].text 
                        dimension = date_parser(dimension, codes['FREQ'])
                        values = observation.xpath(".//generic:ObsValue",
                                namespaces=self.tree.nsmap)
                        value = values[0].values()
                        value = value[0]
                        observation_status = 'A'
                        for attribute in \
                                observation.iterfind(".//generic:Attributes",
                                        namespaces=self.tree.nsmap):
                                    for observation_status_ in \
                                            attribute.xpath(
                                                    ".//generic:Value[@concept='OBS_STATUS']",
                                                    namespaces=self.tree.nsmap):
                                                if observation_status_ is not None:
                                                    observation_status \
                                                            = observation_status_.get('value')
                        time_series_.append((dimension, value, observation_status))
                    time_series_.sort()
                    dates = numpy.array(
                            [observation[0] for observation in time_series_])
                    values = numpy.array(
                            [observation[1] for observation in time_series_])
                    time_series_ = pandas.Series(values, index=dates)
                    self._time_series[str(uuid.uuid1())] = (codes, time_series_)
        return self._time_series


class Concept(object):  # ok
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._concept = None

    @property
    def conceptdata(self):  #ok
        #pudb.set_trace()
        if not self._concept:
            self._concept = {}
            for concept in self.tree.iterfind(".//structure:Concept",
                    namespaces=self.tree.nsmap):
                id = concept.get('id')
                agencyID = concept.get('agencyID')
                version = concept.get('version')
                name = concept.xpath('.//structure:Name', namespaces=self.tree.nsmap)[0].text
                self._concept[id] = (agencyID, version, name)
        return self._concept


class Keyfamily(object): # done
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._codes = None

    @property
    def codes(self):
        if not self._codes:
            self._codes = {}
            codelists = self.tree.xpath(".//message:CodeLists",
                    namespaces=self.tree.nsmap)
            for codelists_ in codelists:
                for codelist in codelists_.iterfind(".//structure:CodeList",
                        namespaces=self.tree.nsmap):
                    name = codelist.xpath('.//structure:Name', namespaces=self.tree.nsmap)
                    name = name[0]
                    name = name.text
                    code = []
                    for code_ in codelist.iterfind(".//structure:Code",
                            namespaces=self.tree.nsmap):
                        code_key = code_.get('value')
                        code_name = code_.xpath('.//structure:Description',
                                namespaces=self.tree.nsmap)
                        code_name = code_name[0]
                        code.append((code_key,code_name.text))
                    self._codes[name] = code
        return self._codes


class Categoryscheme(object): # done
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._category = None

    #def walktree

    @property
    def codes(self):
        if not self._category:
            self._category = {}
            codelists = self.tree.xpath(".//message:CategorySchemes",
                    namespaces=self.tree.nsmap)
            for codelists_ in codelists:
                for codelist in codelists_.iterfind(".//structure:CategoryScheme",
                        namespaces=self.tree.nsmap):
                    name = codelist.xpath('.//structure:Name', namespaces=self.tree.nsmap)
                    name = name[0]
                    name = name.text
                    code = []
                    for code_ in codelist.iterfind(".//structure:Category",
                            namespaces=self.tree.nsmap):
                        code_key = code_.get('id')
                        code_name = code_.xpath('.//structure:Name',
                                namespaces=self.tree.nsmap)
                        code_name = code_name[0]
                        dataflowref=[]
                        #pdb.set_trace()
                        #for dataflow in
                        #code_.iterchildren('DataflowRef'):#todo
                        for dataflow in code_.iterfind(".//structure:DataflowRef", namespaces=self.tree.nsmap):
                            if dataflow is not None:
                                 dataflowID = dataflow.xpath('.//structure:DataflowID', namespaces=self.tree.nsmap)[0].text
                                 agencyID = dataflow.xpath('.//structure:AgencyID', namespaces=self.tree.nsmap)[0].text
                                 version = dataflow.xpath('.//structure:Version', namespaces=self.tree.nsmap)[0].text
                                 dataflowref.append((agencyID, version,
                                     dataflowID))
                                 code.append((code_key,code_name.text,dataflowref))
                    self._category[name] = code
        return self._category


class Organisationschemes(object): #TO TEST
    def __init__(self, SDMXML):
        self.tree = SDMXML
        self._organisationscheme = None

    @property
    def codes(self):
        if not self._organisationscheme:
            self._organisationscheme = {}
            codelists = self.tree.xpath(".//message:OrganisationSchemes",
                    namespaces=self.tree.nsmap)
            for codelists_ in codelists:
                for codelist in codelists_.iterfind(".//structure:CodeList",
                        namespaces=self.tree.nsmap):
                    name = codelist.xpath('.//structure:Name', namespaces=self.tree.nsmap)
                    name = name[0]
                    name = name.text
                    code = []
                    for code_ in codelist.iterfind(".//structure:Agency",
                            namespaces=self.tree.nsmap):
                        code_key = code_.get('id')
                        code_name = code_.xpath('.//structure:Name',
                                namespaces=self.tree.nsmap)
                        code_name = code_name[0]
                        code.append((code_key,code_name.text))
                    self._organisationscheme[name] = code
        return self._organisationscheme


class SDMX_REST(object): 

    def __init__(self, sdmx_url, agencyID):
        self.sdmx_url = sdmx_url
        self.agencyID = agencyID
        self._dataflow = None
        self._organisationscheme = None  
    
    def dataflow(self,resourceID = None): #ok
        if not self._dataflow:
            if resourceID is not None :
                resource = 'Dataflow'
                url = (self.sdmx_url+'/' 
                   + resource + '/'
                   + resourceID)             
                self._dataflow = Dataflows(query_rest(url))
            else :
                resource = 'Dataflow'  
                url = (self.sdmx_url + '/'
                   + resource  )
                self._dataflow = Dataflows(query_rest(url))    
        return self._dataflow

    @property
    def data_organisationscheme(self): #in progress
        if not self._organisationscheme:
            resource = 'OrganisationScheme'  
            url = (self.sdmx_url + '/'
            + resource  )
            self._organsiationscheme = Organisationschemes(query_rest(url))    
        return self._organisationscheme

    def data_extraction(self, flowRef, freq, key,  startperiod=None,
            endperiod=None):#ok
        resourcetype = 'GenericData'
        resource ='dataflow'
        if freq is not None and startperiod is not None and endperiod is not None :
            query = (self.sdmx_url + '/'
                + resourcetype + '?'
                + resource + '='
                + flowRef + '&'
                + 'FREQ=' +  freq 
                + '&CURRENCY='+ key
                + '&startTime=' + startperiod
                + '&endTime=' + endperiod)
        else:
            query = (self.sdmx_url + '/'
                + resourcetype + '?'
                + resource + '='
                + flowRef)
        url = (query)
        return Data(query_rest(url))

    def data_concept(self, flowRef=None):#done
        resource = 'Concept'
        if flowRef is not None :
            url = (self.sdmx_url + '/'
               + resource + '/' 
               + flowRef+ '/'
               +self.agencyID)
        else :
            url = (self.sdmx_url + '/'
               + resource) 
        return Concept(query_rest(url))

    def data_codelist(self, flowRef):#done
        resource = 'CodeList'
        url = (self.sdmx_url + '/'
               + resource + '/' 
               + flowRef+ '/'
               +self.agencyID)
        return Codelist(query_rest(url))

    def data_keyfamily(self, flowRef=None):#done
        resource = 'KeyFamily'
        if flowRef is not None :
            url = (self.sdmx_url + '/'
               + resource + '/' 
               + flowRef)
        else :
            url = (self.sdmx_url + '/'
               + resource) 
        return Keyfamily(query_rest(url))

    def data_categoryscheme(self, flowRef=None):#done
        resource = 'CategoryScheme'
        if flowRef is not None :
            url = (self.sdmx_url + '/'
               + resource + '/' 
               + flowRef)
        else :
            url = (self.sdmx_url + '/'
               + resource) 
        return Categoryscheme(query_rest(url))

    def data_definition(self, flowRef):#todo
        resource = 'CodeList'
        url = (self.sdmx_url + '/'
               + resource + '/' 
               + flowRef)
        return DSD(query_rest(url))


ECB = SDMX_REST('http://sdw-ws.ecb.europa.eu','ECB')

