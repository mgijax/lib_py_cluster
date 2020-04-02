#
#  convert.py
###########################################################################
#
#  Purpose:  This script will convert a cluster set file with multiple
#            cluster members IDs per line into a cluster set file with one
#            cluster member ID per line.
#            
#  Usage:  convert.py  Input-Cluster-File  Output-Cluster-File  Delimiter
#
#      Input-Cluster-File:   The file containing the cluster set.
#      Output-Cluster-File:  The file to contain the converted cluster set.
#      Delimiter:            The delimiter between cluster member IDs in
#                                the input file.
#
#      Examples:
#          convert.py  inputfile  outputfile  ' '
#          convert.py  inputfile  outputfile  ','
#
#  Inputs:
#
#      Input-Cluster-File:
#
#          This input file provides the associations between the clusters
#          IDs and cluster members IDs in the cluster set.  It should have
#          the following format:
#
#          Cluster ID<tab>Cluster Member ID List
#
#          where Cluster Member ID List is a list of one or more cluster
#          members separated by a specified delimiter.
#
#          For Example:
#
#              cid1<tab>cmid11
#              cid2<tab>cmid21 cmid22
#              cid3<tab>cmid31 cmid32 cmid33
#
#  Outputs:
#
#      Output-Cluster-File:
#
#          This output file will contain the converted input file.
#
#          For example, the above input file will be converted to the
#          following output file if the specified delimiter is a space:
#
#              cid1<tab>cmid11
#              cid2<tab>cmid21
#              cid2<tab>cmid22
#              cid3<tab>cmid31
#              cid3<tab>cmid32
#              cid3<tab>cmid33
#
#  Env Vars:  None
#
#  Exit Codes:  0:  Successful completion
#               1:  An exception occurred
#
#  Assumes:  There is a tab character between the cluster ID and the list
#            of cluster member IDs.
#
#  Notes:  Each line of the input file must have a cluster ID and at least
#          1 cluster member ID.  If not, the input line will be ignored
#          and nothing will be included in the output file for that line.
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  01/30/2003  DBM  Initial development
#
###########################################################################

import sys
import os
import regex
import regsub

#
#  Global Variables
#
usage = "USAGE: " + sys.argv[0] + \
        " Input-Cluster-File  Output-Cluster-File  Delimiter"

inputfile = ""   # The cluster set input file.
outputfile = ""  # The cluster set output file.
delimiter = ""   # The delimiter between cluster members of the input file.


#
#  MAIN
#

#
#  Make sure all the arguments to the script have been supplied.
#
if (len(sys.argv) == 4):
    inputfile = sys.argv[1]
    outputfile = sys.argv[2]
    delimiter = sys.argv[3]
else:
    print(usage)
    sys.exit(1)

#
#  Make sure the input file exists.
#
if (not os.path.exists(inputfile)):
    print("Input file does not exist: %s" % inputfile)
    sys.exit(1)

#
#  Make sure the output file does not already exists.
#
if (os.path.exists(outputfile)):
    print("Output file already exists: %s" % outputfile)
    sys.exit(1)

#
#  Open the input and output files.
#
inFile = open(inputfile, 'r')
outFile = open(outputfile, 'w')

#
#  Convert each line in the input file to 1 or more lines in the
#  output file.
#
for line in inFile.readlines():
    #
    #  Replace the whitespace with a pipe character between the cluster ID
    #  and the list of cluster member IDs.  Ignore any line that does not
    #  contain the proper whitespace separating the fields.
    #
    line = regsub.sub('[ 	]+','|',line)
    pipe = regex.search('|',line)
    if (pipe < 0):
        continue

    #
    #  Ignore any line that does not contain a cluster ID.
    #
    if (line[0] == '|'):
        continue

    #
    #  Locate and save the cluster ID.
    #
    cid = line[:pipe]

    #
    #  Get the list of cluster member IDs from the remainder of the line
    #  and ignore the line if there is no list.
    #
    list = regsub.gsub('\n','',line[pipe+1:])
    if (len(list) == 0):
        continue

    #
    #  Extract the cluster member IDs from the list one at a time and write
    #  them to the output file with the cluster ID.  The cluster member IDs
    #  should be separated by the specified delimiter character.
    #
    pos = regex.search(delimiter,list)
    while (pos >= 0):
        outFile.write(cid + "	" + str.strip(list[:pos]) + "\n")
        list = list[pos+1:]
        pos = regex.search(delimiter,list)
    outFile.write(cid + "	" + str.strip(list) + "\n")

#
#  Close the input and output files.
#
inFile.close()
outFile.close()


sys.exit(0)

###########################################################################
#
# Warranty Disclaimer and Copyright Notice
# 
#  THE JACKSON LABORATORY MAKES NO REPRESENTATION ABOUT THE SUITABILITY OR 
#  ACCURACY OF THIS SOFTWARE OR DATA FOR ANY PURPOSE, AND MAKES NO WARRANTIES, 
#  EITHER EXPRESS OR IMPLIED, INCLUDING MERCHANTABILITY AND FITNESS FOR A 
#  PARTICULAR PURPOSE OR THAT THE USE OF THIS SOFTWARE OR DATA WILL NOT 
#  INFRINGE ANY THIRD PARTY PATENTS, COPYRIGHTS, TRADEMARKS, OR OTHER RIGHTS.  
#  THE SOFTWARE AND DATA ARE PROVIDED "AS IS".
# 
#  This software and data are provided to enhance knowledge and encourage 
#  progress in the scientific community and are to be used only for research 
#  and educational purposes.  Any reproduction or use for commercial purpose 
#  is prohibited without the prior express written permission of The Jackson 
#  Laboratory.
# 
# Copyright 1996, 1999, 2002, 2005 by The Jackson Laboratory
# 
# All Rights Reserved
#
###########################################################################
