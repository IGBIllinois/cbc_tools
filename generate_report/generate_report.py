#!/usr/bin/env python
import os
import re
import sys
import subprocess
import xlsxwriter
import json
import xml.etree.ElementTree as ET
import csv
import argparse
from pathlib import Path
import shutil
import time
import pprint

#used to require an existing file in argparse
def file_path(string):
    if os.path.isfile(string):
        return(string)
    else:
        print("ERROR "+string+" is not an existing file")
        sys.exit()

#used to require an existing directory path in argparse
def dir_path(string):
    if os.path.isdir(string):
        return(string)
    else:
        print("ERROR "+string+" is not an existing directory")
        sys.exit()

#used to require an existing outputdir path in argparse
#directory cannot exist, but the parent directory must exist
def outputdir_path(string):
    my_directory=Path(string)
    if not os.path.isdir(string) and os.path.isdir(my_directory.parent) and not os.path.isfile(string):
        return(string)
    else:
        if os.path.isdir(string):
            print("ERROR: directory "+string+" already exists")
        elif not os.path.isdir(my_directory.parent):
            print("ERROR: Parent directory for "+string+" does not exist")
        elif os.path.isfile(string):
            print("ERROR: "+string+" is a preexisting file")
        else:
            print("ERROR: "+string+" output path error")
        sys.exit()

#function to return a hash containing the project, sample, and an array of files in a sample
#primary key is project
#secondary key is sample
#sample key is an array of files in the sample
def getDataFiles(directory,project):
    projectFileList={}
    if args.verbose: print("Top directory is "+directory)
    #get directories in top direcory and look for projects
    if(project == "ALL"):
        projects=[d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory,d)) and re.match("^Project",d)]
    else:
        if os.path.isdir(directory+"/"+project):
            projects=[project]
        else:
            print("ERROR: Project "+project+" does not exist in directory "+directory)
            sys.exit()
    for project in projects:
        projectFileList[project]={}
        if args.verbose: print("Searching "+os.path.join(directory,project))
        samples=[sample for sample in os.listdir(os.path.join(directory,project)) if os.path.isdir(os.path.join(directory,project,sample))]
        for sample in samples:
            projectFileList[project][sample]=[]
            fileNames=[fileName for fileName in os.listdir(os.path.join(directory,project,sample)) if os.path.isfile(os.path.join(directory,project,sample,fileName))]
            for fileName in fileNames:
                if args.verbose: print(project+"\t"+sample+"\t"+fileName)
                projectFileList[project][sample].append(fileName)
    return projectFileList

#make tasks to create .tar.gz files to post to user of each project directory
def tarTaskList(files,resultDirectory,directory,tasks):
    tasklist=[]
    for project in files:
        tasklist.append({'procs':10, 'command':"cd "+directory+" && tar -cf - ./"+project+" | pigz -p10 -c >"+os.path.join(resultDirectory,project+'.tar.gz')})
    return(tasklist)

#make tasks to get the counts of from each fastq.gz file
def getCounts(files, resultDirectory, directory):
    tasklist=[]
    for project in files:
        if args.verbose: print(project)
        for sample in files[project]:
            for fileName in files[project][sample]:
                tasklist.append({'procs':1,'command':"unpigz -p1 -c "+os.path.join(directory,project,sample,fileName)+' | grep -c \'@\''})
    return(tasklist)

#makes tasks to run the falco QC program
def runFalco(files, resultDirectory, directory):
    tasklist=[]
    #todo make falco output directory if it doesnt exist
    for project in files:
        #todo make project output directory if doesnt exist
        for sample in files[project]:
            #here is the problem
            os.makedirs(os.path.join(resultDirectory,'falco',project,sample), exist_ok=True)
            for fileName in files[project][sample]:
                tasklist.append({'procs':1,'command':"falco -q -D "+os.path.join(resultDirectory,'falco',project,sample,fileName+'data.txt')+" -R "+os.path.join(resultDirectory,'falco',project,sample,fileName+'report.html')+" -S "+os.path.join(resultDirectory,'falco',project,sample,fileName+'summary.txt')+" "+os.path.join(directory,project,sample,fileName)})
    return(tasklist)

#run all the commands in the tasklist
def runCommands(commands,tasks):
    runningProcs=0
    procs=[]
    procCount=[]
    results={}
    for command in commands:
        if args.verbose: print("command to run")
        if args.verbose: print(command)
        #if adding the next process will exceeed tasks, wait for a previously running process to end
        while runningProcs+command['procs']>tasks:
            if args.verbose: print("wait, queue is full")
            #check if any procs have ended
            procarrayNumber=0
            for proc in procs:
                #Check if any procs are no longer running
                #capture output and remove from the running process count and lists
                if proc.poll() is not None:
                    output=proc.stdout.readline()
                    #toss the old proc info
                    oldProcess=procs.pop(procarrayNumber)
                    #decrease number of running processes
                    runningProcs=runningProcs-procCount[procarrayNumber]
                    oldCommand=procCount.pop(procarrayNumber)
                    results[oldProcess.args]=output
                    #exit the for loop
                    break
                #else process is still running, check the next one
                #move to the next element in running procs to see if is still running
                procarrayNumber+=1
            #wait one second so this loop does not burn resources
            time.sleep(.5)
        #run a command
        if args.verbose: print("run the command")
        x=subprocess.Popen(command['command'], stdout=subprocess.PIPE,text=True,shell=True)
        #keep track of command info, this needs to be more complex and hanve the number of processes added with it
        procs.append(x)
        procCount.append(command['procs'])
        #increase runningProcs to show that we are using more of our allocation
        runningProcs=runningProcs+command['procs']
    while runningProcs>0:
        if args.verbose: print("wait, not all processes have finished yet")
        procs[0].wait()
        output=procs[0].stdout.readline()
        oldProcess=procs.pop(0)
        runningProcs=runningProcs-procCount[0]
        oldCommand=procCount.pop(0)
        results[oldProcess.args]=output
    return(results)

#generate the xlsx report
def makeReport(comandResults, xlsxfile, protocolFile,downloadFile,sequencer,runNumber,numCycles):
    if args.verbose: print("do make report")
    countArray={}
    #get the count job outputs
    for result in commandResults.keys():
        if re.match('unpigz -p1 -c', result):
            #parse out the filename
            tempstring=re.sub(r'^unpigz -p1 -c ','',result)
            tempstring=re.sub(r" \| grep -c '@'$",'',tempstring)
            patharray=re.split('/',tempstring)
            if patharray[-3] not in countArray:
                countArray[patharray[-3]]={}
            if patharray[-2] not in countArray[patharray[-3]]:
                countArray[patharray[-3]][patharray[-2]]={}
            #make a hash for the run-sample-file
            countArray[patharray[-3]][patharray[-2]][patharray[-1]]=commandResults[result].strip()
    #open the xlsx output file
    workbook=xlsxwriter.Workbook(xlsxfile)
    #make a sheet in the xlsx file
    worksheet=workbook.add_worksheet()
    #formats for fields
    headingFmt=workbook.add_format({'bold': True, 'bg_color': "#00ffff",'align':'center','valign':'vcenter','text_wrap': True,'border':1})
    boldFmt=workbook.add_format({'bold': True,'align':'center','num_format':'#,##0','border':1})
    commonFmt=workbook.add_format({'num_format':'#,##0','border':1})
    blankFmt=workbook.add_format({'border':1})
    footerSectionFmt=workbook.add_format({'bold': True, 'font_color': 'blue','align':'left','font_size': 14})
    row=0
    #print out the headings
    headings=["Project","Lane","Sample Name","Fastq.gz file name","Number of Reads","Tar File Name\n*Link will be active for 7 days*","Comments and Instructions"]
    column=0
    worksheet.set_row(0,30)
    for heading in headings:
        worksheet.write(row,column,heading, headingFmt)
        column+=1
    row += 1

    #Set column widths
    worksheet.set_column("A:A",40)
    worksheet.set_column("B:B",10)
    worksheet.set_column("C:C",20)
    worksheet.set_column("D:D",50)
    worksheet.set_column("E:E",20)
    worksheet.set_column("F:F",50)
    worksheet.set_column("G:G",70)

    #print out the data for counts
    for run in sorted(countArray.keys()):
        runstart=row
        #for each run we need to keep track of the total reads so that we have a number to pass to excel as the initial value
        totRead=0
        worksheet.write(row,0,run)
        worksheet.write(row,5,run+".tar.gz")
        worksheet.write(row,6,"To decompress type: tar -xzvf "+run+".tar.gz")
        for sample in sorted(countArray[run].keys()):
            worksheet.write(row,2,sample,blankFmt)
            for gzFile in sorted(countArray[run][sample].keys()):
                worksheet.write(row,3,gzFile,blankFmt)
                #print the lane number if the first line of the sample row
                if runstart == row:
                    lane=re.findall(r'_L00(\d)_', gzFile)[0]
                    worksheet.write(row,1,lane,blankFmt)
                #add sum the total reads for each run
                if re.search("R[0-9]_001.fastq.gz", gzFile):
                    totRead+=int(countArray[run][sample][gzFile])
                worksheet.write(row,4,int(countArray[run][sample][gzFile]),commonFmt)
                if args.verbose: print(run+"\t"+sample+"\t"+gzFile+"\t"+countArray[run][sample][gzFile])
                row +=1
        worksheet.write(row,3,"Total Reads",boldFmt)
        worksheet.write_formula('E'+str(row+1),'=SUM(FILTER(E'+str(runstart+1)+':E'+str(row)+',REGEXTEST(D'+str(runstart+1)+':D'+str(row)+',"R[0-9]_001.fastq.gz")))',boldFmt,totRead)
        row +=1
        #fill in any empty cells with boarders
        worksheet.conditional_format('A2:G'+str(row),{'type':'blanks', 'format':blankFmt})
    #add a blank line between protocol section and sample section
    row+=2
    #Write the Library_Preparation_and_Sequencing header
    worksheet.write(row,0,"Library Preparation and Sequencing",footerSectionFmt)
    row+=2
    #write the read length
    worksheet.write(row,0,"Reads are "+numCycles+"nt in length.")
    #write the sequencer and run info
    worksheet.write(row,3,sequencer+" run "+runNumber)
    row+=1
    row=importCSVData(row,protocolFile,worksheet)
    #write the download info
    row+=1
    worksheet.write(row,0,"File Download Instructions",footerSectionFmt)
    row+=2
    row=importCSVData(row,downloadFile,worksheet)

    #everything complete, close the file
    workbook.close()

#open a json file and return the data
def getJsonData(jsonFile):
    with open(jsonFile,'r') as filedata:
        data=json.load(filedata)
    return data

#open an xml file and get the data
def getXmlData(xmlFile):
    tree=ET.parse(xmlFile)
    return tree

#imports CSV data from a file, used in making the footer of the excel report
def importCSVData(row, fileName, worksheet):
    with open(fileName,'r', newline='') as csvfile:
        csv_reader=csv.reader(csvfile)
        for csvRow in csv_reader:
            column=0
            for csvColumn in csvRow:
                worksheet.write(row,column,csvColumn)
                column+=1
            row+=1
    return row

#pulls information about the run and the sequencer from the xml file
def getSequencerData(xmlFile):
    xmlData=getXmlData(xmlFile)
    runInfo=xmlData.find('Run')
    sequencer=re.findall(r'\d{8}_(\w+)_\d{4}_\w{10}',runInfo.attrib['Id'])[0]
    runNumber=runInfo.attrib['Number']
    readInfo=runInfo.find('Reads')
    readlist=readInfo.find('Read')
    numCycles=int(readlist.attrib['NumCycles'])
    numCycles=str(numCycles-1)
    return(numCycles, sequencer, runNumber)

#checks to see if tar, pigz, and falco are in the path
def programCheck():
    if shutil.which('tar') is None:
        print("ERROR: tar is not in the path")
        sys.exit()
    if shutil.which('pigz') is None:
        print("ERROR: pigz is not in the path")
        sys.exit()
    if shutil.which('falco') is None:
        print("ERROR: falco is not in the path")
        sys.exit()

if __name__ == "__main__":
    parser=argparse.ArgumentParser(
        prog="PostingPrep",
        description="A program that prepares sequence data for posting by generating the .tar.gz files to post along with the excel report and qc files from falco",
        epilog='Epilog text here')
    parser.add_argument('-i', '--inputdir', type=dir_path, help='input directory which will hold the project files')
    parser.add_argument('-o', '--outputdir', type=outputdir_path, help='output directory to put results into')
    parser.add_argument('-s', '--samplesheet', type=file_path, help='sample sheet to use')
    parser.add_argument('-p', '--project', type=str, default="ALL", help='individual project directory to run, default all directories starting with "Project"')
    parser.add_argument('-th', '--threads', type=int, default=20, help='number of threads to use, default 20')
    parser.add_argument('-skipTar', '--skipTar', action='store_true', help='skip the creation of tar files')
    parser.add_argument('-skipQC', '--skipQC', action='store_true', help='skip the QC step')
    parser.add_argument('-v', '--verbose', action='store_true', help='enable verbose output')
    parser.add_argument('--sequencers', type=file_path, help='json file that contains serial number-to-text info for sequencers, if not specified will attempt to read environment variable SEQUENCERS for file')
    args = parser.parse_args()

    #check if tar, pigz and falco are in path, exits if they are not
    programCheck()

    #read in sequencer serial to name information from sequencers json file
    #This file is passed through either the --sequencers flag (first) or the SEQUENCERS environment flag (second)
    if args.sequencers is not None:
        sequencerInfo=getJsonData(args.sequencers)
    elif os.environ['SEQUENCERS'] is not None:
        sequencerInfo=getJsonData(os.environ['SEQUENCERS'])
    else:
        print("ERROR: Sequencer JSON file not specified by command line (--sequencer) or by environmental variable (SEQUENCER) exiting")
        sys.exit()

    #check if RunInfo file exists
    if os.path.isfile(str(Path(args.inputdir).parent.absolute())+"/RunInfo.xml"):
        #parse xml file to get sequencer serial,run number, and read length
        [numCycles, sequencer, runNumber]=getSequencerData(str(Path(args.inputdir).parent.absolute())+"/RunInfo.xml")
    else:
        print("ERROR: could not find the RunInfo.xml file")
        sys.exit()

    #find all the files we need to work with
    files=getDataFiles(args.inputdir,args.project)

    #start with an empty task list
    tasklist=[]

    #add a task to make a .tar.gz for the project directories
    if not args.skipTar:
        tasklist=tasklist+tarTaskList(files,args.outputdir,args.inputdir,args.threads)

    #task to get counts for each file in the project
    tasklist=tasklist+getCounts(files,args.outputdir,args.inputdir)

    #task to run falco on all .fastq.gz files
    if not args.skipQC:
        #this will make the output directories because falco has to put its files somewhere
        tasklist=tasklist+runFalco(files,args.outputdir, args.inputdir)
    else:
        #if not running falco, then we need to make the main output directory
        os.mkdir(args.outputdir)

    #copy over the sample sheet
    if args.samplesheet is not None:
        shutil.copy(args.samplesheet, args.outputdir)

    #run all the commands
    commandResults=runCommands(tasklist,args.threads)

    #make the falco tarball(s)
    #cannot be run as part of runCommands because the falco files are created in runCommands
    if not args.skipQC:
        for project in files:
            if args.verbose: print(os.path.join(args.outputdir,project)+"_falco.tar.gz")
            subprocess.run("cd "+args.outputdir+" && tar -czf "+project+"_falco.tar.gz "+os.path.join('falco',project), shell=True)
    #create the report - in progress - working on formatting
    makeReport(commandResults,args.outputdir+"/report.xlsx",args.inputdir+'/Protocol.txt',args.inputdir+"/DownloadInstructions.txt",sequencerInfo[sequencer],runNumber, numCycles)
