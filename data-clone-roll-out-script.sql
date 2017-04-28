#------------------------------------------------------------------------------------------------------------------------------
# PROCEDURES
#------------------------------------------------------------------------------------------------------------------------------

DELIMITER $$

DROP PROCEDURE IF EXISTS data_clone$$
CREATE PROCEDURE data_clone (IN schemaname varchar(50), IN rowlimit Int, IN includefieldname Boolean, IN tempcleanup Boolean)
Begin

Declare sql_statement varchar(10000);

Set SQL_SAFE_UPDATES = False;
Set @NewLine = '\r\n';
Set @Tab = '\t';

Set @format_script = False;
Set @row_limit = rowlimit;

#Create table to house the names of tables to be specifically included or excluded (empty if all tables required)
Create Table if not exists data_clone_table_instructions 
	( 
	tbl_name varchar(100) NOT NULL,
    instruction char(7)
	) Engine = InnoDB
;
	
#Create table to house tables to script out
Drop Table if exists data_clone_table_list;
Create Table data_clone_table_list 
	( 
	id int AUTO_INCREMENT,
    tbl_name varchar(100) NOT NULL,
    record_count int NOT NULL,
    clone_used boolean NOT NULL,
    clone_status varchar(20) NOT NULL,
    process_start_dt timestamp NULL,
    process_end_dt timestamp NULL,
	Primary Key (id)
	) Engine = InnoDB;


#Create table to house insert statements
Drop Table if exists data_clone_results;
Create Table data_clone_results 
	( 
	tbl_name varchar(100) NOT NULL,
    row_number int NOT NULL,
    ins_statement varchar(10000) NOT NULL
	) Engine = InnoDB;

#Create temp table to house fields belonging to a table to script out
Drop Table if exists data_clone_temp_field_list;
Create Table data_clone_temp_field_list 
	( 
	id int AUTO_INCREMENT,
    f_name varchar(100) NOT NULL,
	f_type varchar(50) NOT NULL,
	Primary Key (id)
	) Engine = InnoDB;

#Insert into temp table tables to script out
Insert Into data_clone_table_list
	(
    tbl_name
    ,record_count
    ,clone_used
    ,clone_status
    )
	Select 
		TABLE_NAME
		,TABLE_ROWS
        ,FALSE
        ,'WAITING'
	From 
		INFORMATION_SCHEMA.TABLES
	Where
		TABLE_SCHEMA = schemaname
	And
		TABLE_TYPE = 'BASE TABLE'
	And
        TABLE_NAME Not Like 'data_clone_%'
	; 

#Set the total number of tables for the loop
Select
	Count(*)
Into
	@table_count
From
	data_clone_table_list
;

#Capture whether the inclusions table is being used
Select
	Count(*)
Into
	@inclusions
From
	data_clone_table_instructions
Where
	instruction = 'INCLUDE'
;

Set @script_insert = '';
Set @t = 0;

TableLoop: LOOP
    
    SET @t = @t + 1;
    
    IF @t > @table_count THEN 
    
         LEAVE TableLoop;
         
    ELSE
		
        Set @script_current_table = '';
        Set @auto_increment_field = '';
        
        #Set current table name
        Select tbl_name Into @current_table From data_clone_table_list Where id = @t;
        
        #Exclude tables in the exclusions table or are not in the inclusions table where it is being used
        If Exists (Select 1 From data_clone_table_instructions Where instruction = 'EXCLUDE' And tbl_name = @current_table) Or (@inclusions > 0 And Not Exists (Select 1 From data_clone_table_instructions Where instruction = 'INCLUDE' And tbl_name = @current_table)) Then
		
			Update data_clone_table_list Set clone_status = 'EXCLUDED', process_start_dt = Now(), process_end_dt = Now() Where id = @t;
            ITERATE TableLoop;
        
        #Ignore empty tables
        ElseIf (Select record_count From data_clone_table_list Where tbl_name = @current_table) = 0 Then
        
            Update data_clone_table_list Set clone_status = 'IGNORED', process_start_dt = Now(), process_end_dt = Now() Where id = @t;
            ITERATE TableLoop;
		
		Else
        
			Update data_clone_table_list Set clone_status = 'STARTED', process_start_dt = Now() Where id = @t;
        
        End If;
        
        Select COLUMN_NAME Into @auto_increment_field From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = schemaname And TABLE_NAME = @current_table And EXTRA = 'auto_increment';
        
        Select Case When IfNull(@auto_increment_field,'') = '' Then 'No field set' Else @auto_increment_field End Into @auto_increment_field;

        If @auto_increment_field = 'No field set' Then
			
            Drop Table If Exists data_clone_temp_table;
            
            #Write the entire table into a temp table
			Set sql_statement = Concat('Create Table data_clone_temp_table (Select * From ',@current_table,' Where 1<>1);');
			Select sql_statement Into @sql_statement;
			Prepare SQLCommand From @sql_statement;
			Execute SQLCommand;
            
			#Add an incremental ID to identify each row
			Alter Table data_clone_temp_table ADD COLUMN temp_clone_id bigint NOT NULL PRIMARY KEY; 
            
            Set @RowNumber := 0;
            
            #Add data from table
            Set sql_statement = Concat('Insert Into data_clone_temp_table Select *,@RowNumber := @RowNumber + 1 From ', @current_table,';');
			Select sql_statement Into @sql_statement;
            
			Prepare SQLCommand From @sql_statement;
			Execute SQLCommand;
            
            #Set the table to be based off the temp clone table
            #Set @current_table = 'data_clone_temp_table';            
            Set @auto_increment_field = 'temp_clone_id';
			Update data_clone_table_list Set clone_used = True Where id = @t;
	
        End If;

        Select clone_used Into @use_clone_table From data_clone_table_list Where id = @t;
        
        #Prepare field variables
        Truncate Table data_clone_temp_field_list;
        
        #Insert into temp table fields to script out
		Insert Into data_clone_temp_field_list
			(
			f_name
			,f_type
			)
		Select 
			COLUMN_NAME
			,DATA_TYPE
		From 
			INFORMATION_SCHEMA.COLUMNS
		Where
			TABLE_SCHEMA = schemaname
		And
			TABLE_NAME = Case @use_clone_table When True Then 'data_clone_temp_table' Else @current_table End
		And
			COLUMN_NAME != 'temp_clone_id'
		Order By
			ORDINAL_POSITION
		; 
        
		#Set the total number of fields for the table in this loop
		Select
			Count(*)
		Into
			@field_count
		From
			data_clone_temp_field_list
		;
          
         #Set field variables for field loop
         Set @f = 0;
         Set @field_list = '';
         Set @f_name = '';
         
         FieldLoop: Loop
         
			SET @f = @f + 1;
         
			IF @f > @field_count THEN 

				LEAVE FieldLoop;
			
            Else
					
                Select f_name Into @f_name From data_clone_temp_field_list Where id = @f;
                
				If @format_script = True Then 
			
					Set @field_list = Concat(@field_list,',`',@f_name,'`',@NewLine,@Tab);
				Else
					Set @field_list = Concat(@field_list,',`',@f_name,'`');
				
				End If;
           
                ITERATE FieldLoop;
			
            End If;
       
         END LOOP FieldLoop;

		#Adjust field variables to remove spurious characters (like commas)
        Set @field_list = Substring(@field_list,2,Length(@field_list));
		
        #Prepare table for row-level scripting
      
        #Set the current table (use clone table if required)
		Select Case @use_clone_table When True Then 'data_clone_temp_table' Else tbl_name End Into @t_name From data_clone_table_list Where id = @t; 
		
        If @format_script = True Then 
			
			Set @script_insert = Concat('Insert Into `',@current_table,'`',@NewLine,@Tab,'(',@NewLine,@Tab,@field_list,')');
            
		Else
        
			Set @script_insert = Concat(@script_insert,'Insert Into `',@current_table,'` (',@field_list,')');

		End If;
		
        Set sql_statement = Concat('Select Min(',@auto_increment_field,') Into @d From ',schemaname,'.',@t_name,';');
		Select sql_statement Into @sql_statement;
		Prepare SQLCommand From @sql_statement;
		Execute SQLCommand;
        
        Set sql_statement = Concat('Select Max(',@auto_increment_field,') Into @max_increment From ',schemaname,'.',@t_name,';');
        Select sql_statement Into @sql_statement;
		Prepare SQLCommand From @sql_statement;
		Execute SQLCommand;
        
        Set @row_no = 1;
        Set @record_exists = True;
       
		DataLoop: Loop 
        
			Set sql_statement = Concat('Select Case When Count(*) < 1 Then False Else True End Into @record_exists From ',schemaname,'.',@t_name,' Where ',@auto_increment_field,' = ',@d,';');          
			Select sql_statement Into @sql_statement;
			Prepare SQLCommand From @sql_statement;
			Execute SQLCommand;

			If @d > @max_increment Or (@row_limit <> 0 And @row_no > @row_limit) Then 
			
				Leave DataLoop;
			
            ElseIf @record_exists = True Then
				
                #Reset field variables for data field loop
				Set @f = 0;
				Set @f_name = '';
                Set @current_data_row = Concat(@script_insert,@NewLine,'(Select ');
                
				DataFieldLoop: Loop
				
					Set @f = @f + 1;

					If @f > @field_count Then 
						
                        Leave DataFieldLoop;

					Else

						Select f_name Into @f_name From data_clone_temp_field_list Where id = @f;

					End If;
                   
					If (Select Upper(f_type) From data_clone_temp_field_list Where f_name = @f_name) = 'BIT' Then
                    
						Set sql_statement = Concat('Select Case ',@f_name,' When 0 Then ''CLONE TO BIT0'' When 1 Then ''CLONE TO BIT1'' Else ''CLONE TO NULL'' End Into @field_value From ',schemaname,'.',@t_name,' Where ',@auto_increment_field,' = ',@d,';');
                    
                    Else
                    
						Set sql_statement = Concat('Select IfNull(Convert(',@f_name,',char),''CLONE TO NULL'') Into @field_value From ',schemaname,'.',@t_name,' Where ',@auto_increment_field,' = ',@d,';');
					
                    End If;

					Select sql_statement Into @sql_statement;
					Prepare SQLCommand From @sql_statement;
					Execute SQLCommand;
                    
					If includefieldname = True Then
						
						Set @current_data_row = Concat(@current_data_row,'''',Replace(@field_value,'''',''''''),''' As ',@f_name,',');
					
					Else
					
						Set @current_data_row = Concat(@current_data_row,'''',Replace(@field_value,'''',''''''),''',');
					
					End If;
                    
                    Iterate DataFieldLoop;
				
				END LOOP DataFieldLoop;
                
                #Tidy up the record
                Select Replace(@current_data_row,'''CLONE TO NULL''','NULL') Into @current_data_row;
                Select Replace(@current_data_row,'''CLONE TO BIT0''','0') Into @current_data_row;
                Select Replace(@current_data_row,'''CLONE TO BIT1''','1') Into @current_data_row;
                Set @current_data_row = Left(@current_data_row,Length(@current_data_row)-1);
				Set @current_data_row = Concat(@current_data_row,');',@NewLine);	
                Select Replace(@current_data_row,',);',');') Into @current_data_row;
                
				#Insert results into table
				Insert Into data_clone_results
					(
					tbl_name
                    ,row_number
					,ins_statement
					)
				Select
					@current_table
                    ,@row_no
					,@current_data_row
				;
                
                #Increment the physical row count of the table
                Set @row_no = @row_no + 1;
			End If;

			Set @d = @d + 1;
			Iterate DataLoop;

		END LOOP DataLoop;
        
        #Clean up
        Drop Table If Exists data_clone_temp_table;
        Set @script_insert = '';
        Update data_clone_table_list Set clone_status = 'COMPLETED', process_end_dt = Now() Where id = @t;
		ITERATE TableLoop;
    END IF;
END LOOP TableLoop;

#Clean up
If tempcleanup = True Then

	Drop Table If Exists data_clone_temp_field_list;

End If;

Select 
    ins_statement
From
	data_clone_results
;

End$$


#------------------------------------------------------------------------------------------------------------------------------------------------------


DELIMITER $$

DROP PROCEDURE IF EXISTS data_clone_table_instructions$$
CREATE PROCEDURE data_clone_table_instructions (IN instruction varchar(7), IN schemaname varchar(50), IN tablename varchar(102))
Begin

#Create table to house the names of tables to be specifically included or excluded (empty if all tables required)
Drop Table if exists data_clone_table_instructions;
Create Table data_clone_table_instructions 
	( 
	tbl_name varchar(100) NOT NULL,
    instruction char(7)
	) Engine = InnoDB
;

#Tidy up exclusion instruction
If Upper(instruction) In ('E', 'X', 'EX', 'EXCLUDE') Then 
	
    Set instruction = 'EXCLUDE'; 

End If;

#If instruction is not exclusion, set instruction to inclusion
If Upper(instruction) != 'EXCLUDE' Then 

	Set instruction = 'INCLUDE'; 

End If;

If tablename Like '%*%' Then 

	Set tablename = Replace(tablename,'*','%');
    
    Insert Into data_clone_table_instructions 
		(
		tbl_name
        ,instruction
		)
	Select
		TABLE_NAME
        ,instruction
	From 
		INFORMATION_SCHEMA.TABLES
	Where
		TABLE_TYPE = 'BASE TABLE'
    And
		TABLE_SCHEMA = schemaname
	And
		TABLE_NAME Like tablename
	;
		

Else

	Insert Into data_clone_table_instructions 
		(
		tbl_name
        ,instruction
		)
	Select
		tablename
        ,instruction
	From
		Dual
	Where
		Not Exists (Select 1 From data_clone_table_instructions Where tbl_name = tablename)
	;

End If;

End$$