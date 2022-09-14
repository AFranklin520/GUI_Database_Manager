#Anthony Franklin
#Franklin_SLO1.py
#09/13/2022

#Simple Python GUI program to perform CRUD operations on local and NOSQL DBs
#User can also utilize datasets and import those to a table in the DBs
#User can filter data and save it to a table and/or export it to csv, json, or xml
#MONGO_DB SYNC IS COMPLETELY UNTESTED - USE AT YOUR OWN RISK


import tkinter as tk
from tkinter import *
from tkinter import ttk
from tkinter import simpledialog
from tkinter.font import BOLD
from tkinter.ttk import *
from ttkwidgets import Table
from tkinter.messagebox import QUESTION, askokcancel, askyesno, showinfo, INFO, WARNING, showwarning
import os
from os import listdir
import sys
from tkinter import filedialog as fd
import sqlite3 as sl
import pandas as pd
import ttkwidgets.frames as tkf
from pymongo import MongoClient

headers = []
data = []
tablename = ''
conn = sl.connect('SLO1.db')
cur = conn.cursor()

mongo_synced = False

#WORKING
def create_db_table():
    global headers
    global tablename
    name = simpledialog.askstring('Table Name', 'Please enter the name of your new table: ')
    if name != '':
        cols = ''
        for h in headers:
            cols += h + ', '
        cur.execute('drop table if exists {}'.format(name))
        query = "CREATE TABLE {}({})".format(name, cols[:-2])
        cur.execute(query)
        tablename = name
    else:
        showwarning('[ERROR]', 'Table name cannot be blank!')
#WORKING
def get_tabledata():
    results = cur.execute('SELECT * FROM {}'.format(tablename))
    for i in results.description:        
        headers.append(i[0])
    for row in results:
        data.append(row)
#Presents list of tables in local DB to load
def list_tables():
    global tablename
    sql_query = """SELECT name FROM sqlite_master WHERE type='table';"""
    cur.execute(sql_query)
    tables = ''
    i = 1
    res = cur.fetchall()
    if len(res) > 0:
        for t in res:
            tables +='{}: {}\n'.format(i, t)
            i += 1
        tab = simpledialog.askinteger('Open Table', 'Enter the number of the table you wish to view: \n{}'.format(tables))
        tablename = res[tab-1][0]
        get_tabledata()
    else:
        showwarning('[ERROR]', 'The database has no tables in it!\nPlease choose another option:', default='ok')
        get_choice()
#More or less working     
def filter_DBtable(query):
    global data
    name = tablename+ '_filtered'
    q = 'SELECT * FROM {} WHERE {}'.format(tablename, query)
    try:
        name = cur.execute(q)
        if name is not None:
            data = []
            headers = []
            for i in name.description:        
                headers.append(i[0])
            for row in name:
                data.append(row)
            reset_table()
    except:
        showwarning('Nada!', 'Your filter yieled no results', icon=WARNING)
#WORKING
def select_file():
    filetypes = [
        ('DataSets', '*.json *.csv *.xml')
    ]  
    filename = fd.askopenfilename(
        title='Open a file',
        initialdir='./DataSets',
        filetypes=filetypes)
    if filename.endswith('.xml'):
        processXML(filename)
    elif filename.endswith('.json'):
        processJSON(filename)
    elif filename.endswith('.csv'):
        processCSV(filename)
#WORKING
def processCSV(filename):
    global headers
    global data
    global tablename
    headers = []
    data = []

    output = pd.read_csv(filename)
    tablename = 'csv_table'
    output.to_sql(tablename, conn, if_exists='replace', index=False)
    get_tabledata()

"""    #with open(filename, newline='') as csv_file:
        #csv_reader = csv.reader(csv_file)
        #for row in csv_reader:
           # data.append(row)
   # headers = data[0]
    #data.pop(0)
    #create_table()
"""
#WORKING
def processXML(filename):
    global headers
    global data
    global tablename
    headers = []
    data = []

    output = pd.read_xml(filename)
    tablename = 'xml_table'
    output.to_sql(tablename, conn, if_exists='replace', index=False)
    get_tabledata()
"""    #with open(filename, newline='') as f:
        #count = 0
        #rows = []
        #xml = f.read()
        #tree = objectify.fromstring(xml)
        #for p in tree.getchildren():
            #for c in p.getchildren():
                #if count == 0:
                    #headers.append(c.tag)
                #rows.append(c.text)
            #data.append(rows)
            #count += 1
            #rows = []
"""

#WORKING
def processJSON(filename):
    global headers
    global data
    global tablename
    headers = []
    data = []

    output = pd.read_json(filename)
    tablename = 'json_table'
    output.to_sql(tablename, conn, if_exists='replace', index=False)
    get_tabledata()
"""     with open(filename, newline='') as json_file:
        temp = json.load(json_file)
        rows = []
        for key in temp[0]:
            headers.append(key)
        for dict in temp:
            for k, v in dict.items():
                rows.append(v)
            data.append(rows)
            rows = [] 
"""
#WORKING
def create_table():
    global headers
    global data
    headers = []
    data = []
    column_count = simpledialog.askinteger("Table Details", "How many columns will be in your table?", minvalue=1)
    for i in range(column_count):
        col = simpledialog.askstring("Table Details", "Please enter the title of the column at index {}".format(i))
        headers.append(col)
    try:
        create_db_table()
        reset_table()
    except:
        print(Exception)
#WORKING
def export_to_csv():
    try:
        sql_query = pd.read_sql_query('select * from {}'.format(tablename), conn)
        df = pd.DataFrame(sql_query)
        df.to_csv (fd.asksaveasfilename(defaultextension='.csv', filetypes=[('CSV Files', '*.csv')], initialdir='./DataSets', initialfile='{}'.format(tablename) ,title='Choose a filename to save your data to:'), index=False)
    except:
        showwarning('[ERROR]', 'Unable to export table to dataset!\nWe have no more info') 
#WORKING
def export_to_json():
    try:
        sql_query = pd.read_sql_query('select * from {}'.format(tablename), conn)
        df = pd.DataFrame(sql_query)
        df.to_json (fd.asksaveasfilename(defaultextension='.json', filetypes=[('JSON Files', '*.json')], initialdir='./DataSets', initialfile='{}'.format(tablename),title='Choose a filename to save your data to:'), orient='records', index=False)
    except:
        showwarning('[ERROR]', 'Unable to export table to dataset!\nWe have no more info') 
#WORKING
def export_to_xml():
    try:
        sql_query = pd.read_sql_query('select * from {}'.format(tablename), conn)
        df = pd.DataFrame(sql_query)
        df.to_xml(fd.asksaveasfilename(defaultextension='.xml', filetypes=[('XML Files', '*.xml')], initialdir='./DataSets', initialfile='{}'.format(tablename),title='Choose a filename to save your data to:'), index=False)
    except:
        showwarning('[ERROR]', 'Unable to export table to dataset!\nWe have no more info') 

#UNTESTED!
mon_db = MongoClient()
def sync_mongo():
    global mongo_synced
    global mon_db
    port = 'mongodb://localhost:27017'
    p = simpledialog.askstring('MongoDB', 'Please enter the path for your MongoDB connection:\n(Leave blank to use the default - mongodb://localhost:27017)')
    if p != '':
        port = p
    client = MongoClient("{}".format(port), connect=True)
    temp = 'SLO1.db'
    db = simpledialog.askstring('MongoDB Name', 'Please enter the name of the DB you wish to sync to:\nLeave blank to use the same name as the local DB')
    if db == '': db = temp
    mon_db = client['{}'.format(tablename)]
    if mon_db is not None:
        showinfo('[SUCCESS]','Connection Established')
        mongo_synced = True
    else:
        showwarning('[ERROR]','Connection FAILED')

choice = 0
def get_choice():
    global choice
    choice = simpledialog.askinteger("DataSet","What would you like to do:\n1. Open DataSet\n2. Create New Table\n3. Open Table", minvalue=1, maxvalue=3)
    match choice:
        case 1:
            select_file()
        case 2:
            create_table()
        case 3:
            list_tables()
 
get_choice()
if choice > 0 and choice < 4:        
    root = Tk()
    root.geometry('800x600')
    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    style = ttk.Style()
    # Pick A Theme
    style.theme_use('default')
    # Configure the Treeview Colors
    style.configure("Table",
	background="#D3D3D3",
	foreground="black",
	rowheight=25,
	fieldbackground="#D3D3D3")
    root.title('Database Management Console')
    sortable = tk.BooleanVar(root, True)
    drag_row = tk.BooleanVar(root, False)
    drag_col = tk.BooleanVar(root, False)
    
    
    table = Table(root, sortable=True, drag_rows=False, drag_cols=False ,columns=headers,height=6)
    tip = tkf.Tooltip(table, headertext='Edit', text='Right click any cell to edit => Enter to save, Esc to exit', timeout=3, background='#fef9cd')
    tip.bind(table.column)
    for col in headers:
        table.heading(col, text=col)
        table.column(col, width=100, stretch=False)
    # Create Striped Row Tags
    table.tag_configure('oddrow', background="white")
    table.tag_configure('evenrow', background="lightblue")
    # add scrollbars
    sx = tk.Scrollbar(root, orient='horizontal', command=table.xview)
    sy = tk.Scrollbar(root, orient='vertical', command=table.yview)
    table.configure(yscrollcommand=sy.set, xscrollcommand=sx.set)
    table.grid(sticky='ewns')    
    table.bind("<Button-1>", lambda event: show_widget(event))
    table.bind("<Button-3>", lambda event: right_click(table, event))
    #table.bind("<FocusOut>", lambda event: hide_widget(event))
    sx.grid(row=1, column=0, sticky='ew')
    sy.grid(row=0, column=1, sticky='ns')
    root.update_idletasks()
    
    #Populate table from dataset 
    def popTable(dataset):
        #populate table from data list
        global count
        count = 0
        for record in dataset:
            if count % 2 == 0:
                table.insert(parent='', index='end', text='', values=record, tags=('evenrow'))
            else:
                table.insert(parent='', index='end', text='', values= record, tags=('oddrow'))
            # increment counter
            count += 1

    #Query table and update table with results        
    def filter_table():
        colname = simpledialog.askstring('Column Title(s)', "Type the column name to filter by:")
        oper = simpledialog.askstring('Operator Sign', "Type the operator you want to use to compare:\n('+', '-', '>=', etc)")
        filter = simpledialog.askstring('Filter', "Type the value to operate against:")
        if colname != '' and oper != '' and filter != '':
            query = colname + ' ' +  oper + ' '+ filter
        filter_DBtable(query)
    found_it = False    
    to_search = tk.StringVar() 
    _detached = set()
    def _columns_searcher(event):
        if event.keysym == 'Return': return
        global _detached 
        if to_search.get() != '':
            query = to_search.get()
            children = list(table.get_children())
            _brut_searcher(children, query)
        else:
            if len(_detached) > 0:
                for row in _detached:
                    table.reattach(row, '', END)
    def _brut_searcher(children, query):
        global found_it
        i_r = -1
        for item_id in children:
            text = table.item(item_id)['values']
            if query in text:
                i_r += 1
                table.reattach(item_id, '', i_r)
                found_it = True
            else:
                for i in text:
                    if str(i).startswith(query):
                        i_r += 1
                        table.reattach(item_id, '', i_r)
                        break
                    else:
                        _detached.add(item_id)
                        table.detach(item_id)
    def search_results(event = None):
        global found_it
        ent.delete(0, END)
        if found_it == False:
            if len(_detached) > 0:
                reset_rows()
                showinfo("Query Results", 'No items match your search', icon=INFO, default='ok')
        else: return

    #Reattaches rows removed for search
    def reset_rows():
        ent.delete(0,END)
        for row in _detached:
            table.reattach(row, '', END)
    #Repopulates table from data and headers
    def reset_table():
        table.delete(*table.get_children())
        table.config(columns=headers)
        for h in headers:
            table.heading(h, text = h)
        popTable(data)
        root.update()
    #Choose different file and update data and columns, reset table
    def reset_file():
        select_file()
        reset_table()
    #Delete row from data and save to DB
    def delete_record():
        global data
        sel = table.selection()[0]
        if sel == '': return
        if askokcancel("\u2705", "ARE YOU SURE YOU WANT TO DELETE THIS RECORD?\nTHIS CANNOT BE UNDONE!", icon=WARNING, default='ok'):
            table.delete(sel)
            data = []
            for child in table.get_children():
                data.append(table.item(child)['values'])
            save_changes()
            showinfo("\U0001F44C", 'Record Deleted!', icon=INFO, default='ok')
        else:
            showinfo("\N{thumbs down sign}", 'Record Not Deleted!', icon=INFO, default='ok')
    #Add row to data and save to DB
    def add_record():
        global data
        row = []
        for h in headers:
            col = simpledialog.askstring(h, "Enter {}:".format(h))
            while len(col) == 0:
                showwarning("[ERROR]", "Fields Cannot be Blank")
                col = simpledialog.askstring(h, "Enter {}:".format(h))
            else: row.append(col)
        if len(row) == len(headers):
            table.insert('', END, values=row)
            data.append(row)
        save_changes()

    def hide_widget(event):
        delete_button.pack_forget()
    def show_widget(event):
        delete_button.pack(side='left')
    #Define a function to edit specific cell on right click
    def right_click(self, event):
        # what row and column was clicked on
        rowid = self.identify_row(event.y)
        self.focus(rowid)
        self.selection_set(rowid)
        column = self.identify_column(event.x)

        # get column position info
        x,y,width,height = self.bbox(rowid, column)

        # y-axis offset
        pady = 0

        
	
        # place Entry popup properly
        text = self.item(rowid, 'values')[int(column[1:])-1]
        self.entryPopup = EntryPopup(self, rowid, int(column[1:])-1, text)
        self.entryPopup.place(x=x, y=y+pady, width=width, height=height, anchor='w')
    #Saves changes to DB on Enter key
    class EntryPopup(Entry):
        def __init__(self, parent, iid, column, text, **kw):
            ''' If relwidth is set, then width is ignored '''
            super().__init__(parent, **kw)
            self.tv = parent
            self.iid = iid
            self.column = column
            self.insert(0, text)
            self['exportselection'] = False

            self.focus_force()
            self.bind("<Return>", self.on_return)
            self.bind("<Control-a>", self.select_all)
            self.bind("<Escape>", lambda *ignore: self.destroy())

        def on_return(self, event):            
            rowid = self.tv.focus()
            item = list([*self.tv.item(rowid, 'values')])
            if item[self.column] == self.get():
                self.destroy()
                return
            if askokcancel("\u2705", "Would you like to save these changes?", icon=QUESTION ,default='ok'):
                global data
                item[self.column] = self.get()
                self.tv.item(rowid, values = item)
                data = []
                self.destroy()
                root.update()
                for row in self.tv.get_children():
                    data.append(tuple(self.tv.item(row)['values']))
                save_changes()

                showinfo("\U0001F44C", 'Record Has Been Updated!', icon=INFO)
            else:
                showinfo("\N{thumbs down sign}", 'Record Not Changed!', icon=INFO)

        def select_all(self, *ignore):
            
            ''' Set selection on the whole text '''
            self.selection_range(0, 'end')

            # returns 'break' to interrupt default key-bindings
            return 'break'
    #Writes to DB table and commits
    def save_changes():
        global tablename
        global mon_db
        title = simpledialog.askstring('Table Name', 'Choose a name for this table:\nIf you do not enter a name, the current table will be overwritten!')
        if title != '': tablename = title
        cur.execute('DELETE FROM {}'.format(tablename))
        qm = ''
        for i in range(len(headers)-1):
            qm += '?, '
        qm += '?'
        cur.executemany('INSERT INTO {} values({})'.format(tablename, qm),(data))
        conn.commit()
        reset_table()
        #UNTESTED!
        if sync_mongo:
            temp = conn.execute('select * from {}'.format(tablename))
            mon_db.drop()
            mon_db.insert_many(temp)
    #Select file name and export location for current table => export_to_[choice]()
    def export_table(event = None):
        f_type = simpledialog.askinteger('Filetype', 'What type of dataset would you like to export to:\n1. .csv\n2. .json\n3. .xml', minvalue=1, maxvalue=3)
        match f_type:
            case 1:
                export_to_csv()
                return
            case 2:
                export_to_json()
                return
            case 3:
                export_to_xml()
                return

    reset_table()      
    frame = tk.Frame(root)
    ttk.Button(frame, text = 'OPEN FILE', command=reset_file).pack(side='left', padx=4, pady=6) 
    ttk.Button(frame, text='NEW TABLE', command=create_table).pack(side='left', padx=4, pady=6) 
    ttk.Button(frame, text = 'SAVE TABLE', command=save_changes).pack(side='left', padx=4, pady=6)  
    ttk.Button(frame, text = 'MongoDB', command=sync_mongo).pack(side='left', padx=4, pady=6) 
    ttk.Button(frame, text='EXPORT TABLE', command=export_table).pack(side='left', padx=4, pady=6)  
    delete_button = ttk.Button(frame, text = "DELETE", command=delete_record)
    ent = ttk.Entry(frame, textvariable=  to_search)
    ent.pack(side='right', padx=4, pady=6)
    ent.bind('<KeyRelease>', _columns_searcher) and ent.bind('<Return>', search_results)
    ttk.Label(frame, text="Search Table").pack(side='right', padx=4, pady=6)
    ttk.Button(frame, text='RESET', command=reset_rows).pack(side='left', padx=4, pady=6)

    frame.grid()
    root.mainloop()
    #CLEANUP
    conn.commit()
    cur.close()
    conn.close()
    