#------------------------------------------------------------------------------------------------------------------------------
# PROCEDURES
#------------------------------------------------------------------------------------------------------------------------------

DELIMITER $$

DROP PROCEDURE IF EXISTS clone_data$$
CREATE PROCEDURE clone_data (IN schemaname varchar(50), IN rowlimit Int)
Begin

Declare sql_statement varchar(10000);

Set SQL_SAFE_UPDATES = False;
Set @NewLine = '\r\n';
Set @Tab = '\t';

Set @format_script = False;
Set @row_limit = rowlimit;


#Create temp table to house tables to script out
Drop Table if exists temp_clone_table_list;
Create Table temp_clone_table_list 
	( 
	id int AUTO_INCREMENT,
    t_name varchar(100) NOT NULL,
    r_count int NOT NULL,
    clone_used boolean NOT NULL,
	Primary Key (id)
	) Engine = InnoDB;


#Create temp table to house fields belonging to a table to script out
Drop Table if exists temp_clone_field_list;
Create Table temp_clone_field_list 
	( 
	id int AUTO_INCREMENT,
    f_name varchar(100) NOT NULL,
	Primary Key (id)
	) Engine = InnoDB;

#Insert into temp table tables to script out
Insert Into temp_clone_table_list
	(
    t_name
    ,r_count
    ,clone_used
    )
	Select 
		TABLE_NAME
		,TABLE_ROWS
        ,FALSE
	From 
		INFORMATION_SCHEMA.TABLES
	Where
		TABLE_SCHEMA = schemaname
	And
		TABLE_TYPE = 'BASE TABLE'
	And
		TABLE_NAME Not In ('temp_clone_table','temp_clone_table_list', 'temp_clone_field_list')
	; 


#Set the total number of tables for the loop
Select
	Count(*)
Into
	@table_count
From
	temp_clone_table_list
;

Set @script_tables = '';
Set @script_insert = '';
Set @t = 0;
Set @all_data_rows = '';

TableLoop: LOOP
    
    SET @t = @t + 1;
    
    IF @t > @table_count THEN 
    
         LEAVE TableLoop;
         
    ELSE 
		
        Set @script_current_table = '';
        Set @auto_increment_field = '';
        
        #Set current table name
        Select t_name Into @current_table From temp_clone_table_list Where id = @t;
        
        Select COLUMN_NAME Into @auto_increment_field From INFORMATION_SCHEMA.COLUMNS Where TABLE_SCHEMA = schemaname And TABLE_NAME = @current_table And EXTRA = 'auto_increment';
        
        Select Case When IfNull(@auto_increment_field,'') = '' Then 'No field set' Else @auto_increment_field End Into @auto_increment_field;

        If @auto_increment_field = 'No field set' Then
			
            Drop Table If Exists temp_clone_table;
            
            #Write the entire table into a temp table
			Set sql_statement = Concat('Create Table temp_clone_table (Select * From ',@current_table,' Where 1<>1);');
			Select sql_statement Into @sql_statement;
			Prepare SQLCommand From @sql_statement;
			Execute SQLCommand;
            
			#Add an incremental ID to identify each row
			Alter Table temp_clone_table ADD COLUMN temp_clone_id bigint NOT NULL PRIMARY KEY; 
            
            Set @RowNumber := 0;
            
            #Add data from table
            Set sql_statement = Concat('Insert Into temp_clone_table Select *,@RowNumber := @RowNumber + 1 From ', @current_table,';');
			Select sql_statement Into @sql_statement;
            
			Prepare SQLCommand From @sql_statement;
			Execute SQLCommand;
            
            #Set the table to be based off the temp clone table
            #Set @current_table = 'temp_clone_table';            
            Set @auto_increment_field = 'temp_clone_id';
			Update temp_clone_table_list Set clone_used = True Where id = @t;
		
        End If;
        
        #Select @current_table;
        
        Select clone_used Into @use_clone_table From temp_clone_table_list Where id = @t;
        
        #Prepare field variables
        Truncate Table temp_clone_field_list;
        
        #Insert into temp table fields to script out
		Insert Into temp_clone_field_list
			(
			f_name
			)
		Select 
			COLUMN_NAME
		From 
			INFORMATION_SCHEMA.COLUMNS
		Where
			TABLE_SCHEMA = schemaname
		And
			TABLE_NAME = Case @use_clone_table When True Then 'temp_clone_table' Else @current_table End
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
			temp_clone_field_list
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
				
                Select f_name Into @f_name From temp_clone_field_list Where id = @f;
                
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
		Select Case @use_clone_table When True Then 'temp_clone_table' Else t_name End Into @t_name From temp_clone_table_list Where id = @t; 
		
        If @format_script = True Then 
			
			Set @script_insert = Concat(@script_insert,'Insert Into `',@current_table,'`',@NewLine,@Tab,'(',@NewLine,@Tab,@field_list,')');
            
		Else
        
			Set @script_insert = Concat(@script_insert,'Insert Into `',@current_table,'` (',@field_list,')');

		End If;
		
        Set sql_statement = Concat('Select Min(',@auto_increment_field,') Into @d From ',@t_name,';');
		Select sql_statement Into @sql_statement;
		Prepare SQLCommand From @sql_statement;
		Execute SQLCommand;
        
        Set sql_statement = Concat('Select Max(',@auto_increment_field,') Into @max_increment From ',@t_name,';');
		Select sql_statement Into @sql_statement;
		Prepare SQLCommand From @sql_statement;
		Execute SQLCommand;
        
        Set @row_no = 1;
        Set @record_exists = True;
        
		DataLoop: Loop
            
			Set sql_statement = Concat('Select Case When Count(*) < 1 Then False Else True End Into @record_exists From ',@t_name,' Where ',@auto_increment_field,' = ',@d,';');
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

						Select f_name Into @f_name From temp_clone_field_list Where id = @f;

					End If;
                    
					Set sql_statement = Concat('Select IfNull(Convert(',@f_name,',char),''CLONE TO NULL'') Into @field_value From ',@t_name,' Where ',@auto_increment_field,' = ',@d,';');

					Select sql_statement Into @sql_statement;
					Prepare SQLCommand From @sql_statement;
					Execute SQLCommand;
                    
					Set @current_data_row = Concat(@current_data_row,'''',@field_value,''' As ',@f_name,',');
                    
                    Iterate DataFieldLoop;
				
				END LOOP DataFieldLoop;
                
                #Tidy up the record
                Set @all_data_rows = Concat(@all_data_rows,@NewLine,@current_data_row);
                Select Replace(@all_data_rows,'''CLONE TO NULL''','NULL') Into @all_data_rows;
                Set @all_data_rows = Left(@all_data_rows,Length(@all_data_rows)-1);
				Set @all_data_rows = Concat(@all_data_rows,');',@NewLine);
				
                #Increment the physical row count of the table
                Set @row_no = @row_no + 1;
			End If;

			Set @d = @d + 1;
			Iterate DataLoop;

		END LOOP DataLoop;
        
        Set @script_tables = Concat(@script_current_table,@NewLine,@all_data_rows);
        
        #Clean up
        Drop Table If Exists temp_clone_table;
        Set @script_insert = '';
		ITERATE TableLoop;
    END IF;
END LOOP TableLoop;

#Clean up
Drop Table If Exists temp_clone_field_list;
Drop Table If Exists temp_clone_table_list;

Select 
	#@script_insert
	#,@all_data_rows
    #,@d
    @script_tables
    #,@script_current_table
    #,@field_list
    #,@field_count
    #,@t_name
    #,@f_name
;

End$$