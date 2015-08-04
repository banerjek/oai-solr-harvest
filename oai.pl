#!/usr/bin/perl

use Data::Dumper;
use LWP::Simple;
require Encode;
use strict;
use threads;

my $thread_1 = threads->new(\&processOAI, 'http://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb=', 'ListRecords&metadataPrefix=oai_dc&set=1', 'ListRecords&resumptionToken=', 'omeka-campus');
my $thread_2 = threads->new(\&processOAI, 'http://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb=', 'ListRecords&metadataPrefix=oai_dc&set=2', 'ListRecords&resumptionToken=', 'omeka-hca');
my $thread_3 = threads->new(\&processOAI, 'http://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb=', 'ListRecords&metadataPrefix=oai_dc&set=3', 'ListRecords&resumptionToken=', 'omeka-cori');
my $thread_4 = threads->new(\&processOAI, 'http://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb=', 'ListRecords&metadataPrefix=oai_dc&set=12', 'ListRecords&resumptionToken=', 'omeka-brain');
my $thread_5 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:hca-oralhist', 'ListRecords&resumptionToken=', 'hca-oralhist');
my $thread_6 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:hca-books', 'ListRecords&resumptionToken=', 'hca-books');
my $thread_7 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:hca-cac', 'ListRecords&resumptionToken=', 'hca-cac');
my $thread_8 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:fdadrug', 'ListRecords&resumptionToken=', 'fdadrug');
my $thread_9 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:naturopathic', 'ListRecords&resumptionToken=', 'naturopathic');
my $thread_10 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:primate', 'ListRecords&resumptionToken=', 'primate');
my $thread_11 = threads->new(\&processOAI, 'http://digitalcommons.ohsu.edu/do/oai/?verb=', 'ListRecords&metadataPrefix=dcq&set=publication:etd', 'ListRecords&resumptionToken=', 'etd');

$thread_1->join;
$thread_2->join;
$thread_3->join;
$thread_4->join;
$thread_5->join;
$thread_6->join;
$thread_7->join;
$thread_8->join;
$thread_9->join;
$thread_10->join;
$thread_11->join;

######################################################
# detects OAI respository type and remaps a few fields
######################################################

sub detectSystem {
	my $content = $_[0];

	######################
	# detect omeka systems
	######################
	if ($content =~ /oai-pmh-repository/) {
		$content =~ s/<dc:identifier>([^<]+\/files\/)original\/([^<]+)<\/dc:identifier>/<file_identifier>\1original\/\2<\/file_identifier><thumbnail>\1thumbnails\/\2<\/thumbnail>/g;
		$content =~ s/\.JPG(<\/thumbnails>)/.jpg\1/g;
		}
	######################
	# detect bepress systems
	######################
	if ($content =~ /<dc:identifier>(http:\/\/digitalcommons.ohsu.edu[^<]+)<\/dc:identifier>/) {
		$content =~ s/<dc:identifier>(http:\/\/digitalcommons.ohsu.edu[^<]+)<\/dc:identifier>/<original_file>\1<\/original_file>/g; 
		$content =~ s/<dc:description>([^<]+thumbnail.jpg)<\/dc:description>/<thumbnail>\1<\/thumbnail>/g;
		}
	return $content;
	}	

####################
# Add collection tag
####################
sub addCollection {
	my $content = $_[0];
	my $collection = $_[1];

	my %collection_map;
	$collection_map{'publication:etd'} = 'Scholar Archive';
	$collection_map{'publication:hca'} = 'Historical Collections &amp; Archives';
	$collection_map{'publication:hca-oralhist'} = 'Oral History';
	$collection_map{'publication:hca-books'} = 'Rare Book';
	$collection_map{'publication:hca-cac'} = 'Classic Article';
	$collection_map{'publication:naturopathic'} = 'Naturopathic Medicine';
	$collection_map{'publication:primate'} = 'Oregon National Primate Center Rare Books';

	$collection_map{'1'} = 'Campus Collection';
	$collection_map{'2'} = 'Historical Collections &amp; Archives';
	$collection_map{'3'} = 'Clinical Outcomes Research Initiative';
	$collection_map{'12'} = 'Datasets';

	$collection = $collection_map{$collection};

	$content =~ s/(<\/record>)/<collection>$collection<\/collection>\1/g;
	return $content;
	}

#####################################
# select only records containing DOIs
#####################################
sub doiOnly {
	my $content = $_[0];
	my $newcontent = '';
	my $counter = 0;

	my @records = split /\n/, $content;
	foreach (@records) {
		if (/<doi>/) {
			$newcontent .= $_ . "\n";
			}
		}
	return $newcontent;
	}

################################
# Delete wholly unwanted fields
################################
sub deleteFields {
	my $content = $_[0];
	my $badfield = '';
	my @badfields = ("dc:thesis.degree.name", "dc:thesis.degree.level", "dc:description.abstract", "dc:date.available", "dc:description.note", "dc:relation", "dc:rights", "dc:source");
	
	for $badfield(@badfields) {
		$content =~ s/<$badfield>([^<]*)(<\/$badfield>)//g; 
		}
	return $content;
	}

#############################################
# Changes Dublin Core and made up fields
# into solr schema
#############################################
sub mapFields {
	my $content = $_[0];
	my %field_map;

	$field_map{'collection'} = 'collection';
	$field_map{'dc:creator'} = 'author_display';
	$field_map{'dc:description'} = 'description';
	$field_map{'dc:identifier'} = 'identifier';
	$field_map{'dc:publisher'} = 'publisher';
	$field_map{'dc:subject'} = 'subject_topic_facet';
	$field_map{'dc:thesis.degree.department'} = 'publisher';
	$field_map{'dc:thesis.degree.institution'} = 'publisher';
	$field_map{'dc:thesis.degree.school'} = 'publisher';
	$field_map{'dc:title'} = 'title_display';
	$field_map{'dc:type'} = 'format';
	$field_map{'doi'} = 'doi';
	$field_map{'file_identifier'} = 'original_filename';
	$field_map{'original_file'} = 'original_filename';
	$field_map{'pub_date'} = 'pub_date';
	$field_map{'pub_date_display'} = 'pub_date_display';
	$field_map{'thumbnail'} = 'thumbnail';
	
	foreach my $field (keys %field_map) {
		$content =~ s/<$field>([^<]+)(<\/$field>)/<field name="$field_map{$field}">\1<\/field>/g; 
		#### delete unwanted fields
		if (length($field_map{$field}) == 0) {
			$content =~ s/<$field>([^<]+)(<\/$field>)//g; 
			}
		}
	return $content;

	}

#################################################
# Simplifies the XML to make it easier to process
#################################################
sub cleanContent {
	my $content = $_[0];
	# clean space around tags
	$content =~ s/>\s*/>/g;
	$content =~ s/\s*</</g;
	# remove newlines
	$content =~ s/&#13;//g;
	# remove header	
	$content =~ s/.*<ListRecords>//;
	#split into individual records
	$content =~ s/<\/record>\s*/<\/doc>\n/g;
	$content =~ s/<record>/<doc>/g;

	#normalize dates
	$content =~ s/<dc:date(.created)?>/<pub_date>/g;
	$content =~ s/<\/dc:date(.created)?>/<\/pub_date>/g;
	$content =~ s/(<pub_date>([^<]*)<\/pub_date>)/\1<pub_date_display>\2<\/pub_date_display>/g;
	$content =~ s/<pub_date>[^<]*19th c[^<]*</<pub_date>1800</ig;
	$content =~ s/<pub_date>[^<]*20th c[^<]*</<pub_date>1900</ig;
	$content =~ s/<pub_date>[^<]*(\d{4})[^<]*</<pub_date>\1</g;
	$content =~ s/<pub_date_display>[^<]*(\d{4})[^<]*Z</<pub_date_display>\1</g;
	$content =~ s/<pub_date_display>[^<]*\d*\/?\d*\?(\d{4})[^<]*</<pub_date_display>\1</g;
	$content =~ s/<pub_date>[^<]*no date[^<]*<//ig;
	
	#normalize formats
	$content =~ s/>still image</>Still Image</ig;
	$content =~ s/>s</>Still Image</ig;
	

	# remove unneeded tags 
	$content =~ s/<[^>]*xsd.>//g;
	$content =~ s/\s*<header>.*<metadata>\s*//g;
	$content =~ s/\s*<\/metadata>\s*//g;
	$content =~ s/\s*<\/oai_dc:dc>\s*//g;

	# delete other unneeded fields which are properly structured
	$content = &deleteFields($content);

	# remap doi fields
	$content =~ s/<dc:identifier>(doi:|http:\/\/dx.doi.org\/)([^<]+)<\/dc:identifier>/<doi>doi:\2<\/doi>/g;
	#remap omeka identifiers
	$content =~ s/<dc:identifier>[^<]+\/files\/original\/([^<]+)<\/dc:identifier>/<file_identifier>\1<\/file_identifier>/g;
	#delete unneeded identifiers
	$content =~ s/<dc:identifier>[^<]+\/items\/show\/([^<]+)<\/dc:identifier>//g;
	$content =~ s/<dc:identifier>[^<]+\/etd\/[0-9]+\/([^<]+)<\/dc:identifier>//g;
	#remap dates
	$content =~ s/<(dc:date[^>]*>)(\d{4})[^<]*<\/\1/<\1\2<\/\1/g;

	return $content;
	}

sub processOAI {
	########################################################
	# $oai_base is what OAI-PMH commands will be appended to
	# e.g. $oai_base = 'http://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb='
	########################################################
	my $oai_base = @_[0];
	my $oai_prefix = @_[1];
	my $oai_resumption = @_[2];
	my $collection = @_[3];
	my $display_collection = $collection;
	my $good_to_print = 0;
	my @content;
	my $xmlfile = '';
	
	##############################################################
	# $url is a full URL that retrieves the first records in a set
	# e.g. $url = 'ListRecords&metadataPrefix=oai_dc&set=1'
	##############################################################
	my $url = $oai_base . $oai_prefix;
	
	###############################################################
	# $baseurl is what we expect the resumption token to be added to
	# to retrive the next series of records
	# e.g. $baseurl = $oai_base . 'ListRecords&resumptionToken=';
	###############################################################
	my $baseurl = $oai_base . $oai_resumption;
	my $resumptionToken = '';
	
	##########################################################
	# $collection is used to determine the filename
	# e.g. $collection = omeka_etd 
	##########################################################
	my $counter = 0;
	
	while ($url) {
		my $content = get $url;
		$resumptionToken = '';	
	
		if ($content =~ m/<resumptionToken[^>]+>([^<]+)<\/resumptionToken>/) {
			($resumptionToken) = $1;
			}
	
		my @collections = ($content =~ m/<setSpec[^>]*>([^<]+)<\/setSpec>/g); 
		### remove duplicates from the array of collections
		my %collection_hash   = map { $_ => 1 } @collections;
		my @unique_collections = keys %collection_hash;
	
		### add collection details to records
		foreach $display_collection(@unique_collections) {
			$content = &addCollection($content, $display_collection);
			}
	
		$content = &detectSystem($content);
		$content = &cleanContent($content);
		$content = &doiOnly($content);
		$content = &mapFields($content);
	
		### open the file only if there is something to process 
		if (length($content) > 50) {
			$good_to_print = 1;
			}
			
		if (length($resumptionToken) > 0) {
			$url = $baseurl . $resumptionToken;
			} 
			else 
			{
			undef $resumptionToken;
			undef $url;
			}
		### create a file for every record because solr chokes if any record in a files is bad 
		if ($good_to_print == 1) {
			my @content = split /\n/g, $content;		
			foreach $xmlfile (@content) {
				open (OUTFILE, '>:utf8', 'xml/' . $collection . "." . sprintf("%06d", $counter) . ".xml");
				
				### add format if none provided
					if ($xmlfile !~ m/<field name="format"/) {
						if ($xmlfile =~ m/\.jpg/i) {
							$xmlfile =~ s/<\/doc>/<field name="format">Still Image<\/field><\/doc>/;
							}
						if ($xmlfile =~ m/>Scholar Archive</i) {
							$xmlfile =~ s/<\/doc>/<field name="format">Text<\/field><\/doc>/;
							}
						if ($xmlfile =~ m/>Oral History</i) {
							$xmlfile =~ s/<\/doc>/<field name="format">Text<\/field><\/doc>/;
							}
						###### correct format for Naturopathic medicine
						if ($xmlfile =~ m/>Naturopathic Medicine</i) {
							$xmlfile =~ s/"format">[^<]*</"format">Text</;
							}
						}
	
				### add thumbnail if none provided
					### Oral histories
					if ($xmlfile !~ m/<field name="thumbnail"/) {
						if ($xmlfile =~ m/>Oral History</i) {
							$xmlfile =~ s/<\/doc>/<field name="thumbnail">http:\/\/digitalcollections.ohsu.edu\/files\/thumbnails\/oral_history.png<\/field><\/doc>/;
							}
						}
					### books 
					if ($xmlfile !~ m/<field name="thumbnail"/) {
						if ($xmlfile =~ m/>Naturopathic Medicine</i) {
							$xmlfile =~ s/<\/doc>/<field name="thumbnail">http:\/\/digitalcollections.ohsu.edu\/files\/thumbnails\/book.png<\/field><\/doc>/;
							}
						}
					### theses 
					if ($xmlfile !~ m/<field name="thumbnail"/) {
						if ($xmlfile =~ m/>Scholar Archive</i) {
							$xmlfile =~ s/<\/doc>/<field name="thumbnail">http:\/\/digitalcollections.ohsu.edu\/files\/thumbnails\/paper.png<\/field><\/doc>/;
							}
						}
				print OUTFILE "<add>$xmlfile</add>";
				close(OUTFILE);
				$counter++;
				}
			$good_to_print = 0;
			}
		}
	}	

