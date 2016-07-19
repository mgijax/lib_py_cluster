#!/usr/local/bin/python
#
#  clusterfile.py
###########################################################################
#
#  Purpose:  This script provides a wrapper for the clusterlib bucketizer
#            function.  It accepts two files as input that each contain a
#            cluster set.  Then it calls the bucketizer function to
#            compare the cluster sets and divide them into different
#            output files, depending on their relationships.
#            
#  Usage:  clusterfile.py  Cluster-File1  Cluster-File2  Bucket-Prefix
#
#      Cluster-File1:  The file containing the first cluster set.
#      Cluster-File2:  The file containing the second cluster set.
#      Bucket-Prefix:  The file prefix for the bucketizer output files.
#
#  Inputs:
#
#      Cluster-File1:  This input file provides the associations between
#                      the clusters and cluster members in the first
#                      cluster set.
#
#      Cluster-File2:  This input file provides the associations between
#                      the clusters and cluster members in the first
#                      cluster set.
#
#                      Each input file must have 1 or more records with
#                      the following format:
#
#                      Cluster ID<tab>Cluster Member ID
#
#  Outputs:
#
#      The following files are created as a result of calling the bucketizer
#      to compare the 2 cluster sets:
#
#      bucket.0to1:  Contains records from the second cluster set that are
#                    not in the first cluster set.
#
#      bucket.1to0:  Contains records from the first cluster set that are
#                    not in the second cluster set.
#
#      bucket.1to1:  Contains clusters from each set that are only
#                    associated with each other.
#
#      bucket.1toN:  Contains clusters from the first cluster set that are
#                    associated with multiple clusters from the second
#                    cluster set.
#
#      bucket.Nto1:  Contains clusters from the second cluster set that are
#                    associated with multiple clusters from the first
#                    cluster set.
#
#      bucket.NtoN:  Contains multiple clusters from the first cluster set
#                    that are associated with multiple clusters from the
#                    second cluster set.
#
#  Env Vars:  None
#
#  Exit Codes:  0:  Successful completion
#               1:  An exception occurred
#
#  Assumes:  Nothing
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  09/27/2002  DBM  Initial development
#
###########################################################################

import sys
import os
import string
import clusterlib
import db

#
#  Global Variables
#
usage = "USAGE: " + sys.argv[0] + \
        " Cluster-File1 Cluster-File2 Bucket-Prefix"

clusterfile1 = ""   # The first cluster set input file.
clusterfile2 = ""   # The second cluster set input file.
bucketprefix = ""   # The file prefix for the bucketizer output files.


#
#  MAIN
#

#
#  Make sure all the arguments to the script have been supplied.
#
if (len(sys.argv) == 4):
    clusterfile1 = sys.argv[1]
    clusterfile2 = sys.argv[2]
    bucketprefix = sys.argv[3]
else:
    print usage
    sys.exit(1);

#
#  Use one continuous connection to the database so temporary tables that
#  are created by the bucketizer are not lost between calls to db.sql().
#
db.useOneConnection(1)

#
#  Call the bucketizer to create the output files.
#
clusterlib.bucketize(file1=clusterfile1, file2=clusterfile2,
                     prefix=bucketprefix)

#
#  Close the connection to the database.
#
db.useOneConnection(0)

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
# Copyright © 1996, 1999, 2002 by The Jackson Laboratory
# 
# All Rights Reserved
#
###########################################################################
