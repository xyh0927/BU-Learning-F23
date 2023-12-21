import pandas as pd
import pyodbc as db

conn = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3b;'
                'Trusted_Connection=yes;')

cursor = conn.cursor()

csv_file_path = 'G:/BU_STUDY/METCS689A1/HW3B/CLIWOC15.csv'

df = pd.read_csv(csv_file_path,usecols=['RecID','InstAbbr','InstName','InstPlace','InstLand',
'NumberEntry','NameArchiveSet','ArchivePart','Specification','LogbookIdent',
'LogbookLanguage','EnteredBy','DASnumber','ImageNumber','VoyageFrom',
'VoyageTo','ShipName','ShipType','Company','OtherShipInformation',
'Nationality','Name1','Rank1','Name2','Rank2',
'Name3','Rank3','ZeroMeridian','StartDay','TimeGen',
'ObsGen','ReferenceCourse','ReferenceWindDirection','DistUnits','DistToLandmarkUnits',
'DistTravelledUnits','LongitudeUnits','VoyageIni','UnitsOfMeasurement','Calendar',
'Year','Month','Day','DayOfTheWeek','PartDay',
'TimeOB','Watch','Glasses','UTC','CMG',
'ShipSpeed','Distance','drLatDeg','drLatMin','drLatSec',
'drLatHem','drLongDeg','drLongMin','drLongSec','drLongHem',
'LatDeg','LatMin','LatSec','LatHem','LongDeg','LongMin','LongSec','LongHem','Lat3','Lon3',
'LatInd','LonInd','PosCoastal','EncName','EncNat','EncRem','Anchored','AnchorPlace','LMname1','LMdirection1',
'LMdistance1','LMname2','LMdirection2','LMdistance2','LMname3','LMdirection3','LMdistance3','EstError','ApplError','WindDirection',
'AllWindDirections','WindForce','WindForceScale','AllWindForces','WindScale','Weather','ShapeClouds','DirClouds','Clearness','PrecipitationDescriptor',
'CloudFrac','Gusts','Rain','Fog','Snow','Thunder','Hail','SeaIce','Duplicate','Release',
'SSTReading','SSTReadingUnits','StateSea','CurrentDir','CurrentSpeed','TairReading','AirThermReadingUnits','ProbTair','BaroReading','AirPressureReadingUnits',
'BarometerType','BarTempReading','BarTempReadingUnits','HumReading','HumidityUnits','HumidityMethod','PumpWater','WaterAtThePumpUnits','LifeOnBoard','LifeOnBoardMemo',
'Cargo','CargoMemo','ShipAndRig','ShipAndRigMemo','Biology','BiologyMemo','WarsAndFights','WarsAndFightsMemo','Illustrations','TrivialCorrection','OtherRem'])

df.fillna('unknown', inplace=True)

create_table_statement = '''
IF OBJECT_ID('Trips_Staging', 'U') IS NOT NULL 
    DROP TABLE Trips_Staging

    CREATE TABLE Trips_Staging (
    RecID NVARCHAR(4000), 
    InstAbbr NVARCHAR(4000), 
    InstName NVARCHAR(4000), 
    InstPlace NVARCHAR(4000), 
    InstLand NVARCHAR(4000), 
    NumberEntry NVARCHAR(4000), 
    NameArchiveSet NVARCHAR(4000), 
    ArchivePart NVARCHAR(4000), 
    Specification NVARCHAR(4000), 
    LogbookIdent NVARCHAR(4000), 
    LogbookLanguage NVARCHAR(4000), 
    EnteredBy NVARCHAR(4000), 
    DASnumber NVARCHAR(4000), 
    ImageNumber NVARCHAR(4000), 
    VoyageFrom NVARCHAR(4000), 
    VoyageTo NVARCHAR(4000), 
    ShipName NVARCHAR(4000), 
    ShipType NVARCHAR(4000), 
    Company NVARCHAR(4000), 
    OtherShipInformation NVARCHAR(4000), 
    Nationality NVARCHAR(4000), 
    Name1 NVARCHAR(4000), 
    Rank1 NVARCHAR(4000), 
    Name2 NVARCHAR(4000), 
    Rank2 NVARCHAR(4000), 
    Name3 NVARCHAR(4000), 
    Rank3 NVARCHAR(4000), 
    ZeroMeridian NVARCHAR(4000), 
    StartDay NVARCHAR(4000), 
    TimeGen NVARCHAR(4000), 
    ObsGen NVARCHAR(4000), 
    ReferenceCourse NVARCHAR(4000), 
    ReferenceWindDirection NVARCHAR(4000), 
    DistUnits NVARCHAR(4000), 
    DistToLandmarkUnits NVARCHAR(4000), 
    DistTravelledUnits NVARCHAR(4000), 
    LongitudeUnits NVARCHAR(4000), 
    VoyageIni NVARCHAR(4000), 
    UnitsOfMeasurement NVARCHAR(4000), 
    Calendar NVARCHAR(4000), 
    Year NVARCHAR(4000), 
    Month NVARCHAR(4000), 
    Day NVARCHAR(4000), 
    DayOfTheWeek NVARCHAR(4000), 
    PartDay NVARCHAR(4000), 
    TimeOB NVARCHAR(4000), 
    Watch NVARCHAR(4000), 
    Glasses NVARCHAR(4000), 
    UTC NVARCHAR(4000), 
    CMG NVARCHAR(4000), 
    ShipSpeed NVARCHAR(4000), 
    Distance NVARCHAR(4000), 
    drLatDeg NVARCHAR(4000), 
    drLatMin NVARCHAR(4000), 
    drLatSec NVARCHAR(4000), 
    drLatHem NVARCHAR(4000), 
    drLongDeg NVARCHAR(4000), 
    drLongMin NVARCHAR(4000), 
    drLongSec NVARCHAR(4000), 
    drLongHem NVARCHAR(4000), 
    LatDeg NVARCHAR(4000), 
    LatMin NVARCHAR(4000), 
    LatSec NVARCHAR(4000), 
    LatHem NVARCHAR(4000), 
    LongDeg NVARCHAR(4000), 
    LongMin NVARCHAR(4000), 
    LongSec NVARCHAR(4000), 
    LongHem NVARCHAR(4000), 
    Lat3 NVARCHAR(4000), 
    Lon3 NVARCHAR(4000), 
    LatInd NVARCHAR(4000), 
    LonInd NVARCHAR(4000), 
    PosCoastal NVARCHAR(4000), 
    EncName NVARCHAR(4000), 
    EncNat NVARCHAR(4000), 
    EncRem NVARCHAR(4000), 
    Anchored NVARCHAR(4000), 
    AnchorPlace NVARCHAR(4000), 
    LMname1 NVARCHAR(4000), 
    LMdirection1 NVARCHAR(4000), 
    LMdistance1 NVARCHAR(4000), 
    LMname2 NVARCHAR(4000), 
    LMdirection2 NVARCHAR(4000), 
    LMdistance2 NVARCHAR(4000), 
    LMname3 NVARCHAR(4000), 
    LMdirection3 NVARCHAR(4000), 
    LMdistance3 NVARCHAR(4000), 
    EstError NVARCHAR(4000), 
    ApplError NVARCHAR(4000), 
    WindDirection NVARCHAR(4000), 
    AllWindDirections NVARCHAR(4000), 
    WindForce NVARCHAR(4000), 
    WindForceScale NVARCHAR(4000), 
    AllWindForces NVARCHAR(4000), 
    WindScale NVARCHAR(4000), 
    Weather NVARCHAR(4000), 
    ShapeClouds NVARCHAR(4000), 
    DirClouds NVARCHAR(4000), 
    Clearness NVARCHAR(4000), 
    PrecipitationDescriptor NVARCHAR(4000), 
    CloudFrac NVARCHAR(4000), 
    Gusts NVARCHAR(4000), 
    Rain NVARCHAR(4000), 
    Fog NVARCHAR(4000), 
    Snow NVARCHAR(4000), 
    Thunder NVARCHAR(4000), 
    Hail NVARCHAR(4000), 
    SeaIce NVARCHAR(4000), 
    Duplicate NVARCHAR(4000), 
    Release NVARCHAR(4000), 
    SSTReading NVARCHAR(4000), 
    SSTReadingUnits NVARCHAR(4000), 
    StateSea NVARCHAR(4000), 
    CurrentDir NVARCHAR(4000), 
    CurrentSpeed NVARCHAR(4000), 
    TairReading NVARCHAR(4000), 
    AirThermReadingUnits NVARCHAR(4000), 
    ProbTair NVARCHAR(4000), 
    BaroReading NVARCHAR(4000), 
    AirPressureReadingUnits NVARCHAR(4000), 
    BarometerType NVARCHAR(4000), 
    BarTempReading NVARCHAR(4000), 
    BarTempReadingUnits NVARCHAR(4000), 
    HumReading NVARCHAR(4000), 
    HumidityUnits NVARCHAR(4000), 
    HumidityMethod NVARCHAR(4000), 
    PumpWater NVARCHAR(4000), 
    WaterAtThePumpUnits NVARCHAR(4000), 
    LifeOnBoard NVARCHAR(4000), 
    LifeOnBoardMemo NVARCHAR(4000), 
    Cargo NVARCHAR(4000), 
    CargoMemo NVARCHAR(4000), 
    ShipAndRig NVARCHAR(4000), 
    ShipAndRigMemo NVARCHAR(4000), 
    Biology NVARCHAR(4000), 
    BiologyMemo NVARCHAR(4000), 
    WarsAndFights NVARCHAR(4000), 
    WarsAndFightsMemo NVARCHAR(4000), 
    Illustrations NVARCHAR(4000), 
    TrivialCorrection NVARCHAR(4000), 
    OtherRem NVARCHAR(4000)
)
'''

cursor.execute(create_table_statement)
conn.commit()
cursor.close()
conn.close()
print("Trips_Staging table created successfully!")


conn1 = db.connect('Driver={SQL Server};'
                'Server=XUYUHAN;'
               'Server=SQLServer-PC;'
               'Database=hw3b;'
                'Trusted_Connection=yes;')


cursor1 = conn1.cursor()

insert_query = """
INSERT INTO Trips_Staging (RecID,InstAbbr,InstName,InstPlace,InstLand,NumberEntry,NameArchiveSet,ArchivePart,Specification,LogbookIdent,LogbookLanguage,EnteredBy,DASnumber,ImageNumber,VoyageFrom,VoyageTo,ShipName,ShipType,Company,OtherShipInformation,Nationality,Name1,Rank1,Name2,Rank2,Name3,Rank3,ZeroMeridian,StartDay,TimeGen,ObsGen,ReferenceCourse,ReferenceWindDirection,DistUnits,DistToLandmarkUnits,DistTravelledUnits,LongitudeUnits,VoyageIni,UnitsOfMeasurement,Calendar,Year,Month,Day,DayOfTheWeek,PartDay,TimeOB,Watch,Glasses,UTC,CMG,ShipSpeed,Distance,drLatDeg,drLatMin,drLatSec,drLatHem,drLongDeg,drLongMin,drLongSec,drLongHem,LatDeg,LatMin,LatSec,LatHem,LongDeg,LongMin,LongSec,LongHem,Lat3,Lon3,LatInd,LonInd,PosCoastal,EncName,EncNat,EncRem,Anchored,AnchorPlace,LMname1,LMdirection1,LMdistance1,LMname2,LMdirection2,LMdistance2,LMname3,LMdirection3,LMdistance3,EstError,ApplError,WindDirection,AllWindDirections,WindForce,WindForceScale,AllWindForces,WindScale,Weather,ShapeClouds,DirClouds,Clearness,PrecipitationDescriptor,CloudFrac,Gusts,Rain,Fog,Snow,Thunder,Hail,SeaIce,Duplicate,Release,SSTReading,SSTReadingUnits,StateSea,CurrentDir,CurrentSpeed,TairReading,AirThermReadingUnits,ProbTair,BaroReading,AirPressureReadingUnits,BarometerType,BarTempReading,BarTempReadingUnits,HumReading,HumidityUnits,HumidityMethod,PumpWater,WaterAtThePumpUnits,LifeOnBoard,LifeOnBoardMemo,Cargo,CargoMemo,ShipAndRig,ShipAndRigMemo,Biology,BiologyMemo,WarsAndFights,WarsAndFightsMemo,Illustrations,TrivialCorrection,OtherRem)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

for index, row in df.iterrows():
    Key = index + 1
    
    cursor1.execute(insert_query, row['RecID'],row['InstAbbr'],row['InstName'],row['InstPlace'],row['InstLand'],row['NumberEntry'],row['NameArchiveSet'],row['ArchivePart'],row['Specification'],row['LogbookIdent'],row['LogbookLanguage'],row['EnteredBy'],row['DASnumber'],row['ImageNumber'],row['VoyageFrom'],row['VoyageTo'],row['ShipName'],row['ShipType'],row['Company'],row['OtherShipInformation'],row['Nationality'],row['Name1'],row['Rank1'],row['Name2'],row['Rank2'],row['Name3'],row['Rank3'],row['ZeroMeridian'],row['StartDay'],row['TimeGen'],row['ObsGen'],row['ReferenceCourse'],row['ReferenceWindDirection'],row['DistUnits'],row['DistToLandmarkUnits'],row['DistTravelledUnits'],row['LongitudeUnits'],row['VoyageIni'],row['UnitsOfMeasurement'],row['Calendar'],row['Year'],row['Month'],row['Day'],row['DayOfTheWeek'],row['PartDay'],row['TimeOB'],row['Watch'],row['Glasses'],row['UTC'],row['CMG'],row['ShipSpeed'],row['Distance'],row['drLatDeg'],row['drLatMin'],row['drLatSec'],row['drLatHem'],row['drLongDeg'],row['drLongMin'],row['drLongSec'],row['drLongHem'],row['LatDeg'],row['LatMin'],row['LatSec'],row['LatHem'],row['LongDeg'],row['LongMin'],row['LongSec'],row['LongHem'],row['Lat3'],row['Lon3'],row['LatInd'],row['LonInd'],row['PosCoastal'],row['EncName'],row['EncNat'],row['EncRem'],row['Anchored'],row['AnchorPlace'],row['LMname1'],row['LMdirection1'],row['LMdistance1'],row['LMname2'],row['LMdirection2'],row['LMdistance2'],row['LMname3'],row['LMdirection3'],row['LMdistance3'],row['EstError'],row['ApplError'],row['WindDirection'],row['AllWindDirections'],row['WindForce'],row['WindForceScale'],row['AllWindForces'],row['WindScale'],row['Weather'],row['ShapeClouds'],row['DirClouds'],row['Clearness'],row['PrecipitationDescriptor'],row['CloudFrac'],row['Gusts'],row['Rain'],row['Fog'],row['Snow'],row['Thunder'],row['Hail'],row['SeaIce'],row['Duplicate'],row['Release'],row['SSTReading'],row['SSTReadingUnits'],row['StateSea'],row['CurrentDir'],row['CurrentSpeed'],row['TairReading'],row['AirThermReadingUnits'],row['ProbTair'],row['BaroReading'],row['AirPressureReadingUnits'],row['BarometerType'],row['BarTempReading'],row['BarTempReadingUnits'],row['HumReading'],row['HumidityUnits'],row['HumidityMethod'],row['PumpWater'],row['WaterAtThePumpUnits'],row['LifeOnBoard'],row['LifeOnBoardMemo'],row['Cargo'],row['CargoMemo'],row['ShipAndRig'],row['ShipAndRigMemo'],row['Biology'],row['BiologyMemo'],row['WarsAndFights'],row['WarsAndFightsMemo'],row['Illustrations'],row['TrivialCorrection'],row['OtherRem'])

conn1.commit()

cursor1.close()
conn1.close()

print("Data inserted into Trips_Staging table successfully!")