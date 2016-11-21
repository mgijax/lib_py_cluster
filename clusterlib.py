#  clusterlib.py
###########################################################################
#
#  Purpose:  This module provides the functionality needed to compare 2 sets
#            of clusters and separate the clusters into "buckets" based on
#            their relationships with each other.
#
#            The "bucketize" function is the only function that is intended
#            to be called by another Python script.  The other functions in
#            this module are used internally.
#            
#  Usage:  This module is imported by other scripts the need to use the
#          bucketizer function.
#
#  Inputs:
#
#      The arguments to the bucketize() function are as follows:
#
#      file1:   The first cluster set if a file is used as input.
#      table1:  The first cluster set if the database is used as input.
#      cid1:    The cluster ID column in table1.
#      cmid1:   The cluster member column in table1.
#      file2:   The second cluster set if a file is used as input.
#      table2:  The second cluster set if the database is used as input.
#      cid2:    The cluster ID column in table1.
#      cmid2:   The cluster member column in table1.
#      prefix:  The prefix (including the path) for the bucket files.
#
#      NOTE:  For each cluster set, the input can come from a file OR a
#             table in the database, but NOT BOTH.  Therefore, just the
#             file or table/cid/cmid is supplied to the function for each
#             cluster set.
#
#      If a file is used to input a cluster set, each record in the file
#      must have the following format:
#
#          Cluster-ID<TAB>Cluster-Member-ID
#
#  Outputs:
#
#      The files created by calling the bucketize() function are as follows:
#
#      <prefix>.0to1:  The bucket file containing 0:1 relationships.
#      <prefix>.1to0:  The bucket file containing 1:0 relationships.
#      <prefix>.1to1:  The bucket file containing 1:1 relationships.
#      <prefix>.1toN:  The bucket file containing 1:N relationships.
#      <prefix>.Nto1:  The bucket file containing N:1 relationships.
#      <prefix>.NtoN:  The bucket file containing N:N relationships.
#
#  Env Vars:  None
#
#  Exit Codes:  See individual functions within this module.
#
#  Assumes:  If one or both of the cluster sets for the bucketizer are
#            coming from the database, the calling script should have
#            already established a connection to the database.  In addition,
#            the function "db.useOneConnection(1)" should have been used
#            to ensure that the connection is not automatically closed
#            after each call to "db.sql()".  Otherwise, temporary tables
#            will be lost between processing steps.
#
#            It is also assumed that any cluster sets that come from input
#            files will not contain any duplicates.
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
# 11/21/2016   sc   remove setAutoTranslate*
#
#  07/19/2016  sc   Convert to postgres
#
#  09/23/2002  DBM  Initial development
#
###########################################################################

import sys
import os
import string
import db

db.setTrace(True)

#
#  Global Variables
#
set1_table = None    #  Table that contains the 1st cluster set.
set1_cid = None      #  Cluster ID column in the 1st cluster set.
set1_cmid = None     #  Cluster member ID column in the 1st cluster set.
set2_table = None    #  Table that contains the 2nd cluster set.
set2_cid = None      #  Cluster ID column in the 2nd cluster set.
set2_cmid = None     #  Cluster member ID column in the 2nd cluster set.

clist1 = []          #  List of set 1 clusters that map to set 2 clusters.
clist2 = []          #  List of set 2 clusters that map to set 1 clusters.


#
#  Debugging functions
#
def printSourceCounts():
    cmds = []
    cmds.append("select count(*) from %s" % set1_table)
    cmds.append("select count(*) from %s" % set2_table)
    counts = db.sql(cmds,'auto')
    print "Source 1 records: %d" % counts[0][0]['']
    print "Source 2 records: %d" % counts[1][0]['']
    sys.stdout.flush()

def printLists():
    print clist1
    print clist2
    print
    sys.stdout.flush()


###########################################################################
#  Function:  loadFileSource
#
#  Purpose:  Load a cluster set into a temporary table from a file.
#
#  Arguments:  file - The full path name of the input file that contains
#                     the cluster set.
#              tempno - The counter value to use when creating the temporary
#                       table name.  This gets incremented prior to each
#                       call to this function.
#
#  Returns:  0 - Successful completion
#            1 - An error occurred
#
#  Assumes:  Nothing
#
#  Effects:  Nothing
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def loadFileSource(file, tempno):

    #
    #  Make sure the input file exists and open it.
    #
    if (not os.path.exists(file)):
        print "Input file does not exist: %s" % file
        return 1
    inFile = open(file, 'r')

    #
    #  Create a temp table to load the cluster set into.
    #
    create_stmt = "create temporary table cluster_set%d " % tempno + \
                  "(cid varchar(30), cmid varchar(30))"
    db.sql(create_stmt,None)

    #
    #  Build the SQL statement that will be used to insert the clusters
    #  into the temp table.
    #
    insert_stmt = "insert into cluster_set%d " % tempno + \
                  "values ('%s', '%s')"

    #
    #  Loop through each record in the input file and load the clusters.
    #
    for line in inFile.readlines():
        [cid, cmid] = string.splitfields(line)
        db.sql(insert_stmt % (cid, cmid), None)

    inFile.close()
    return 0


###########################################################################
#  Function:  getLists
#
#  Purpose:  Join two cluster sets that reside in tables using the cluster
#            members for the join condition.  The cluster ID from each set
#            are saved in lists and will eventually be saved in the
#            different bucket files.
#
#  Arguments:  None
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  Adds the cluster IDs to the cluster list variables.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def getLists():
    global clist1, clist2

    #
    #  Find all the clusters from each table that map to each other via
    #  their cluster members.
    #
    map = db.sql("select distinct s1.%s as cid1, s2.%s as cid2 " % (set1_cid, set2_cid) + \
                 "from %s s1, %s s2 " % (set1_table, set2_table) + \
                 "where s1.%s = s2.%s " % (set1_cmid, set2_cmid) + \
                 "order by s2.%s, s1.%s" % (set2_cid, set1_cid),'auto')

    #
    #  Save the cluster IDs in the cluster lists.
    #
    for m in map:
        clist1.append(m['cid1'])
        clist2.append(m['cid2'])

    return 0


###########################################################################
#  Function:  sortLists
#
#  Purpose:  Sort the cluster lists based on the IDs in the first or
#            second list.
#
#  Arguments:  sortby - 1 means to sort by the first list
#                       2 means to sort by the second list
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  Sorts the cluster IDs in the cluster list variables.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def sortLists(sortby):

    #
    #  Determine which cluster list to base the sorting on.
    #
    if (sortby == 1):
        set1 = clist1
        set2 = clist2
    else:
        set1 = clist2
        set2 = clist1

    #
    #  Perform the standard bubble sort on each list, making sure that the
    #  clusters from each list remain paired (same index).
    #
    for i in range(0,len(set1)):
        for j in range(i,len(set1)):
            if (set1[j]+set2[j] < set1[i]+set2[i]):
                temp1 = set1[i]
                temp2 = set2[i]
                set1[i] = set1[j]
                set2[i] = set2[j]
                set1[j] = temp1
                set2[j] = temp2

    return 0


###########################################################################
#  Function:  get0to1
#
#  Purpose:  Find all the clusters in the second cluster set that are not
#            associated with any clusters in the first cluster set.  These
#            clusters are written to the "0:1" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  Nothing
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def get0to1(prefix):

    #
    #  Build the query to find the 0:1 relationships.
    #
    res = db.sql("select s2.%s, s2.%s " % (set2_cid, set2_cmid) + \
                 "from %s s2 " % set2_table + \
                 "where s2.%s not in " % (set2_cid) + \
                     "(select s2.%s " % set2_cid + \
                     "from %s s1, %s s2 " % (set1_table, set2_table) + \
                     "where s1.%s = s2.%s) " % (set1_cmid, set2_cmid) + \
                 "order by s2.%s, s2.%s" % (set2_cid, set2_cmid),'auto')

    #
    #  Create the bucket file write all the results to it.
    #
    outFile = open("%s.0to1" % prefix, 'w')
    for r in res:
        outFile.write("%s\t%s\n" % (r[set2_cid],r[set2_cmid]))
    outFile.close()

    return 0


###########################################################################
#  Function:  get1to0
#
#  Purpose:  Find all the clusters in the first cluster set that are not
#            associated with any clusters in the second cluster set.  These
#            are written to the "1:0" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  Nothing
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def get1to0(prefix):

    #
    #  Build the query to find the 1:0 relationships.
    #
    res = db.sql("select s1.%s, s1.%s " % (set1_cid, set1_cmid) + \
                 "from %s s1 " % set1_table + \
                 "where s1.%s not in " % (set1_cid) + \
                     "(select s1.%s " % set1_cid + \
                     "from %s s1, %s s2 " % (set1_table, set2_table) + \
                     "where s1.%s = s2.%s) " % (set1_cmid, set2_cmid) + \
                 "order by s1.%s, s1.%s" % (set1_cid, set1_cmid),'auto')

    #
    #  Create the bucket file write all the results to it.
    #
    outFile = open("%s.1to0" % prefix, 'w')
    for r in res:
        outFile.write("%s\t%s\n" % (r[set1_cid],r[set1_cmid]))
    outFile.close()

    return 0


###########################################################################
#  Function:  get1to1
#
#  Purpose:  Find clusters from each set that are only associated with each
#            other.  These clusters are written to the "1:1" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  The clusters are deleted from the cluster lists when they are
#            written to the bucket.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def get1to1(prefix):
    global clist1, clist2

    #
    #  Create the bucket file for the 1:1 relationships.
    #
    outFile = open("%s.1to1" % prefix, 'w')

    i = 0
    while (i < len(clist1)):

        #
        #  Find associated clusters that only occur once in their
        #  respective cluster sets.
        #
        if (clist1.count(clist1[i]) == 1 and clist2.count(clist2[i]) == 1):
            outFile.write("%s\t%s\n" % (clist1[i],clist2[i]))
            del clist1[i]
            del clist2[i]
        else:
            i = i + 1

    outFile.close()
    return 0


###########################################################################
#  Function:  get1toN
#
#  Purpose:  Find all the clusters in the first cluster set that are
#            associated with multiple clusters in the second cluster set.
#            These clusters are written to the "1:N" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  The clusters are deleted from the cluster lists when they are
#            written to the bucket.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def get1toN(prefix):
    global clist1, clist2

    #
    #  Create the bucket file for the 1:N relationships.
    #
    outFile = open("%s.1toN" % prefix, 'w')

    #
    #  Loop through the first cluster ID list to find all cluster IDs that
    #  have 1:N relationships with cluster IDs in the second list.
    #
    i = 0
    while (i < len(clist1)):

        #
        #  Save the current cluster ID from the first list and then make a
        #  temporary mapping list of all the cluster IDs in the second list
        #  that map to it.
        #
        c1 = clist1[i]
        nlist = []
        for j in range(i,len(clist1)):
            if (clist1[j] == c1):
                nlist.append(clist2[j])

        #
        #  For each cluster ID in the temporary mapping list, make sure it
        #  maps exclusively to the current cluster ID in the first list.
        #  Otherwise, it would be a N:N relationship, not 1:N.
        #
        match = 1
        if (len(nlist) > 1):
            for j in range(0,len(nlist)):
                if (clist2.count(nlist[j]) > 1):
                    match = 0
                    break
        else:
            match = 0

        #
        #  If a 1:N relationship has been found, go through the first list
        #  starting at the current position and write all the mappings to
        #  the bucket.
        #
        if (match):
            j = i
            while (j < len(clist1)):
                if (clist1[j] == c1):
                    outFile.write("%s\t%s\n" % (clist1[j],clist2[j]))
                    del clist1[j]
                    del clist2[j]
                else:
                    j = j + 1

        #
        #  If the current cluster ID in the first list does not have a 1:N
        #  relationship with the second list, advance the counter to point
        #  to the next cluster ID in the list that is different.
        #
        else:
            i = i + 1
            while (i < len(clist1)):
                if (clist1[i] == c1):
                    i = i + 1
                else:
                    break

    outFile.close()
    return 0


###########################################################################
#  Function:  getNto1
#
#  Purpose:  Find all the clusters in the second cluster set that are
#            associated with multiple clusters in the first cluster set.
#            These clusters are written to the "N:1" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  The clusters are deleted from the cluster lists when they are
#            written to the bucket.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def getNto1(prefix):
    global clist1, clist2

    #
    #  Create the bucket file for the N:1 relationships.
    #
    outFile = open("%s.Nto1" % prefix, 'w')

    #
    #  Loop through the second cluster ID list to find all cluster IDs that
    #  have N:1 relationships with cluster IDs in the first list.
    #
    i = 0
    while (i < len(clist2)):

        #
        #  Save the current cluster ID from the second list and then make a
        #  temporary mapping list of all the cluster IDs in the first list
        #  that map to it.
        #
        c2 = clist2[i]
        nlist = []
        for j in range(i,len(clist2)):
            if (clist2[j] == c2):
                nlist.append(clist1[j])

        #
        #  For each cluster ID in the temporary mapping list, make sure it
        #  maps exclusively to the current cluster ID in the second list.
        #  Otherwise, it would be a N:N relationship, not N:1.
        #
        match = 1
        if (len(nlist) > 1):
            for j in range(0,len(nlist)):
                if (clist1.count(nlist[j]) > 1):
                    match = 0
                    break
        else:
            match = 0

        #
        #  If a 1:N relationship has been found, go through the second list
        #  starting at the current position and write all the mappings to
        #  the bucket.
        #
        if (match):
            j = i
            while (j < len(clist2)):
                if (clist2[j] == c2):
                    outFile.write("%s\t%s\n" % (clist1[j],clist2[j]))
                    del clist1[j]
                    del clist2[j]
                else:
                    j = j + 1

        #
        #  If the current cluster ID in the second list does not have a 1:N
        #  relationship with the first list, advance the counter to point
        #  to the next cluster ID in the list that is different.
        #
        else:
            i = i + 1
            while (i < len(clist2)):
                if (clist2[i] == c2):
                    i = i + 1
                else:
                    break

    outFile.close()
    return 0


###########################################################################
#  Function:  getNtoN
#
#  Purpose:  Find multiple clusters in the first cluster set that are
#            associated with multiple clusters in the second cluster set.
#            These clusters are written to the "N:N" bucket.
#
#  Arguments:  prefix - The prefix for the bucket file.
#
#  Returns:  0 - Successful completion
#
#  Assumes:  Nothing
#
#  Effects:  The clusters are deleted from the cluster lists when they are
#            written to the bucket.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def getNtoN(prefix):
    global clist1, clist2

    #
    #  Create the bucket file for the N:N relationships.
    #
    outFile = open("%s.NtoN" % prefix, 'w')

    #
    #  By process of elimination, any cluster IDs left in the two lists
    #  must be part of N:N relationships because all the 1:1, 1:N and N:1
    #  relationship have already been removed.
    #
    for i in range(0,len(clist1)):
        outFile.write("%s\t%s\n" % (clist1[0],clist2[0]))
        del clist1[0]
        del clist2[0]
    outFile.close()

    return 0


###########################################################################
#  Function:  bucketize
#
#  Purpose:  Compare 2 sets of clusters and divide them into "buckets"
#            based on their relationships.
#
#  Arguments:  file1 - The first cluster set if a file is used as input.
#              table1 - The first cluster set if the database is used as input.
#              cid1 - The cluster ID column in table1.
#              cmid1 - The cluster member column in table1.
#              file2 - The second cluster set if a file is used as input.
#              table2 - The second cluster set if the database is used as input.
#              cid2 - The cluster ID column in table1.
#              cmid2 - The cluster member column in table1.
#              prefix - The prefix (including the path) for the bucket files.
#
#  Returns:  0 - Successful completion
#            1 - An error occurred
#
#  Assumes:  Nothing
#
#  Effects:  The set1_table, set1_cid and set1_cmid variables are set to the
#            table and column names for the first cluster set.
#            The set2_table, set2_cid and set2_cmid variables are set to the
#            table and column names for the second cluster set.
#
#  Throws:  Nothing
#
#  Notes:  None
###########################################################################
def bucketize(file1=None, table1=None, cid1=None, cmid1=None,
              file2=None, table2=None, cid2=None, cmid2=None,
              prefix=None):

    global set1_table, set1_cid, set1_cmid
    global set2_table, set2_cid, set2_cmid

    tempno = 0  #  Counter used to create temporary table names.

    #
    #  Validate the arguments to the function.  There should be a file OR
    #  a table supplied for the first cluster set and a file OR a table
    #  supplied for the second cluster set.
    #
    if (file1 == None) and (table1 == None):
        print "No file or table was given for source 1"
        return 1

    if (file1 != None) and (table1 != None):
        print "A file and table were both given for source 1"
        return 1

    if (table1 != None) and (cid1 == None or cmid1 == None):
        print "Cluster or cluster member column not specified for source 1"
        return 1

    if (file2 == None) and (table2 == None):
        print "No file or table was given for source 2"
        return 1

    if (file2 != None) and (table2 != None):
        print "A file and table were both given for source 2"
        return 1

    if (table2 != None) and (cid2 == None or cmid2 == None):
        print "Cluster or cluster member column not specified for source 2"
        return 1

    if (prefix == None):
        print "Prefix for bucket files not specified"
        return 1

    #
    #  If a file was provided for the first cluster set, load the clusters
    #  into a new temporary table and save the table/column names.
    #
    if (file1 != None):
        tempno = tempno + 1
        if (loadFileSource(file1, tempno) == 0):
            set1_table = "cluster_set%d" % tempno
            set1_cid = "cid"
            set1_cmid = "cmid"
        else:
            return 1

    #
    #  If a database table was provided for the first cluster set, save the
    #  table/column names.
    #
    else:
        set1_table = table1
        set1_cid = cid1
        set1_cmid = cmid1

    #
    #  If a file was provided for the second cluster set, load the clusters
    #  into a new temporary table and save the table/column names.
    #
    if (file2 != None):
        tempno = tempno + 1
        if (loadFileSource(file2, tempno) == 0):
            set2_table = "cluster_set%d" % tempno
            set2_cid = "cid"
            set2_cmid = "cmid"
        else:
            return 1

    #
    #  If a database table was provided for the second cluster set, save the
    #  table/column names.
    #
    else:
        set2_table = table2
        set2_cid = cid2
        set2_cmid = cmid2

    #
    #  Create the 0:1 and 1:0 buckets by getting the cluster IDs in each
    #  set that are not in the other set.
    #
    get0to1(prefix)
    get1to0(prefix)

    #
    #  Join the two cluster sets together by mapping the cluster member IDs.
    #  Create lists of the clusters IDs from each set that map to each other.
    #  The lists are initially sorted based on the cluster IDs in the second
    #  list, so that the N:1 relationship can be found.
    #
    getLists()

    #
    #  Create the N:1 bucket.
    #
    getNto1(prefix)

    #
    #  Re-sort the cluster lists based on the cluster IDs in the first list,
    #  so that the 1:1, 1:N and N:N relationships can be found.
    #
    sortLists(1)

    #
    #  Create the 1:1, 1:N and N:N buckets.
    #
    get1to1(prefix)
    get1toN(prefix)
    getNtoN(prefix)

    return 0

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
