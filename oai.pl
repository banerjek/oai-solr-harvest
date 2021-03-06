#!/usr/bin/perl

use Data::Dumper;
use LWP::Simple;
require Encode;
use utf8;
use strict;
use threads;

my $ohsu_omeka = 'https://digitalcollections.ohsu.edu/oai-pmh-repository/request?verb=';
my $ohsu_bepress = 'http://digitalcommons.ohsu.edu/do/oai/?verb=';

my $thread_1 = threads->new(\&processOAI, $ohsu_omeka, 'ListRecords&metadataPrefix=oai_dc&set=1', 'omeka-campus');
my $thread_2 = threads->new(\&processOAI, $ohsu_omeka, 'ListRecords&metadataPrefix=oai_dc&set=2', 'omeka-hca');
my $thread_3 = threads->new(\&processOAI, $ohsu_omeka, 'ListRecords&metadataPrefix=oai_dc&set=3', 'omeka-cori');
my $thread_4 = threads->new(\&processOAI, $ohsu_omeka, 'ListRecords&metadataPrefix=oai_dc&set=12', 'omeka-brain');
my $thread_5 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:hca-oralhist', 'hca-oralhist');
my $thread_6 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:hca-books', 'hca-books');
my $thread_7 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:hca-cac', 'hca-cac');
my $thread_8 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:fdadrug', 'bepress-fdadrug');
my $thread_9 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:naturopathic', 'bepress-naturopathic');
my $thread_10 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:primate', 'bepress-primate');
my $thread_11 = threads->new(\&processOAI, $ohsu_bepress, 'ListRecords&metadataPrefix=dcq&set=publication:etd', 'bepress-etd');

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
		$content =~ s/\.JPG(<\/thumbnail>)/.jpg\1/g;
		$content =~ s/\.jpeg(<\/thumbnail>)/.jpg\1/g;
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
	$collection_map{'publication:fdadrug'} = 'FDA Drug Approval Documents';
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

########################################################################
# Add virtual collections based on parsing of Source and/or other fields
########################################################################
sub addVirtualCollections {
	my $content = $_[0];
	my $collection = '';
	my @collections = ('Audio Visual Collection', 'Clarice Ashworth Francone Collection', 'Charles F. Norris Photograph Album', "Colonel Strohm's Nurses Photograph Album", 'Esther Pohl Lovejoy Collection', 'George W. King Scrapbook', 'Grace Phelps Papers', 'Herbert Merton Greene Papers', 'Historical Image Collection', 'Jeri L. Dobbs Collection', 'Medical Museum Collection', 'Melvin Paul Judkins Collection', 'Richard B. Dillehunt Photograph Album','Hospital Nursing Images','Strategic Communications Images');

	foreach $collection(@collections) {
		$content =~ s/<dc:source>\s*$collection[^<]*<\/dc:source>/<collection>$collection<\/collection>/g;
		}

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

##################################################################################
# Regex string replacements kept breaking, so this was changed to a string routine
##################################################################################


################################
# Delete wholly unwanted fields
################################
sub deleteFields {
	my $content = $_[0];
	my $badfield = '';
	my $newcontent = '';
	my $begin_pos = 0;
	my $end_pos = 0;
	my $last_pos = 0;

	my @badfields = ("dc:thesis.degree.name", "dc:thesis.degree.level", "dc:description.abstract", "dc:date.available", "dc:description.note", "dc:relation", "dc:rights", "dc:source");
	
	for $badfield(@badfields) {
		$content =~ s/<$badfield>[^<]*<\/$badfield>//g; 
		
		# Regex expression caused core dump so switch to string routine
#		$begin_pos = 0;
#		$end_pos = 0;
#		$last_pos = 0;
#		$newcontent = '';
#
#		while(($begin_pos = index($content, '<' . $badfield . '>', $last_pos)) > -1){
#			$end_pos = index($content, '</' . $badfield . '>', $begin_pos) + length($badfield) + 3; ######## field and content
#			#print substr($content, $begin_pos, $end_pos - $begin_pos) . "\n";
#			$newcontent .= substr($content, $last_pos, $begin_pos - $last_pos - 1);
#			$last_pos = $end_pos + 1; ##### skip over the tag
#			}	
#		if (length($newcontent) > 1) {
#			$content = $newcontent;
#			}
		}
	# handle a few tags differently because they sometimes aren't closed 
	$content =~ s/<(br|p|hr|li)>//g; 
	$content =~ s/<\/(br|p|hr|li)>//g; 
	$content =~ s/<(br|p|hr|li) \/>//g; 
	$content =~ s/<(br|p|hr|li) \/>//g; 
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
		$content =~ s/<$field>([^<]*)<\/$field>/<field name="$field_map{$field}">\1<\/field>/g; 
		#### delete unwanted fields
		if (length($field_map{$field}) == 0) {
			$content =~ s/<$field><\/$field>//g; 
			}
		}
	return $content;

	}

#################################################
# Simplifies the XML to make it easier to process
#################################################
sub cleanContent {
	my $content = $_[0];
	# remove newlines
	$content =~ s/\&#13;//g;

	# clean space around tags
	$content =~ s/>\s*/>/g;
	$content =~ s/\s*</</g;
	# remove header	
	$content =~ s/.*<ListRecords>//;
	#split into individual records
	$content =~ s/<record>/<doc>/g;
	$content =~ s/<\/record>\s*/<\/doc>\n/g;

	#normalize dates
	$content =~ s/<dc:date(.created)?>/<pub_date>/g;
	$content =~ s/<\/dc:date(.created)?>/<\/pub_date>/g;
	$content =~ s/(<pub_date>([^<]*)<\/pub_date>)/\1<pub_date_display>\2<\/pub_date_display>/g;
	$content =~ s/<pub_date>[^<]*19th c[^<]*</<pub_date>1800</ig;
	$content =~ s/<pub_date>[^<]*20th c[^<]*</<pub_date>1900</ig;
	$content =~ s/<pub_date>[^<]*(\d{4})[^<]*</<pub_date>\1</g;
	$content =~ s/<pub_date_display>[^<]*(\d{4})[^<]*Z</<pub_date_display>\1</g;
	$content =~ s/<pub_date_display>[^<]*\d*\/?\d*\?(\d{4})[^<]*</<pub_date_display>\1</g;
	$content =~ s/<pub_date>[^<]*no date[^<]*<\/pub_date>//ig;
	$content =~ s/<pub_date>ND<\/pub_date>//ig;
	
	#normalize formats
	$content =~ s/>still image</>Still Image</ig;
	$content =~ s/>s</>Still Image</ig;
	

	# remove unneeded tags 
	$content =~ s/<[^>]*xsd.>//g;
	$content =~ s/\s*<header>.*<metadata>\s*//g;
	$content =~ s/\s*<\/metadata>\s*//g;
	$content =~ s/\s*<\/oai_dc:dc>\s*//g;

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
	my $collection = @_[2];
	my $display_collection = $collection;
	my $good_to_print = 0;
	my @content;
	my $xmlfile = '';
	
	##############################################################
	# $url is a full URL that retrieves the first records in a set
	# e.g. $url = 'ListRecords&metadataPrefix=oai_dc&set=1'
	##############################################################
	my $url = $oai_base . $oai_prefix;
	my $oai_resumption = 'ListRecords&resumptionToken=';
	
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
		## remove high characters -- will mess up some foreign characters
		$content =~ s/[^[:ascii:]]//g;
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
#print "addCollections\n";
		$content = &addVirtualCollections($content);
#print "deleteFields\n";
		$content = &deleteFields($content);
#print "detectSystem\n";
		$content = &detectSystem($content);
#print "cleanContent\n";
		$content = &cleanContent($content);
#print "doiOnly\n";
		$content = &doiOnly($content);
#print "mapFields\n";
		$content = &mapFields($content);
#print "printOut\n";
	
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
				
				### add collections based on specific fields
			#		if ($xmlfile =~ m/Office of Strategic Communications</i) {
			#			$xmlfile =~ s/<\/doc>/<field name="collection">Strategic Communication Images<\/field><\/doc>/;
			#			}
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
				#### Hack to fix terminating </doc> fields getting stripped somewhere
				$xmlfile = "<add>" . $xmlfile . "</add>";
				$xmlfile =~ s/<\/field><\/add>/<\/field><\/doc><\/add>/;

				print OUTFILE $xmlfile;
				close(OUTFILE);
				$counter++;
				}
			$good_to_print = 0;
			}
		}
	}	

