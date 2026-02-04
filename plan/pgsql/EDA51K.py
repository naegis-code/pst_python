import os
import shutil
import tkinter as tk
from tkinter import messagebox
from tkinter import filedialog

def copy_files():
    # Get the source and destination directories from the GUI
    source = source_entry.get()
    destination = dest_entry.get()

    try:
        # Check if source exists
        if not os.path.exists(source):
            messagebox.showerror("Error", f"Source directory '{source}' does not exist.")
            return
        
        # Ensure destination directory exists
        if not os.path.exists(destination):
            os.makedirs(destination)

        # Copy files
        for item in os.listdir(source):
            s = os.path.join(source, item)
            d = os.path.join(destination, item)
            if os.path.isdir(s):
                shutil.copytree(s, d, dirs_exist_ok=True)
            else:
                shutil.copy2(s, d)
        
        messagebox.showinfo("Success", "Files copied successfully.")
    except Exception as e:
        messagebox.showerror("Error", f"An error occurred: {e}")

def browse_source():
    dir_path = filedialog.askdirectory()
    if dir_path:
        source_entry.delete(0, tk.END)
        source_entry.insert(0, dir_path)

def browse_destination():
    dir_path = filedialog.askdirectory()
    if dir_path:
        dest_entry.delete(0, tk.END)
        dest_entry.insert(0, dir_path)

# Create the main window
root = tk.Tk()
root.title("MTP File Copier")

# Source directory
tk.Label(root, text="Source Directory:").grid(row=0, column=0, padx=10, pady=10)
source_entry = tk.Entry(root, width=50)
source_entry.grid(row=0, column=1, padx=10, pady=10)
tk.Button(root, text="Browse", command=browse_source).grid(row=0, column=2, padx=10, pady=10)

# Destination directory
tk.Label(root, text="Destination Directory:").grid(row=1, column=0, padx=10, pady=10)
dest_entry = tk.Entry(root, width=50)
dest_entry.grid(row=1, column=1, padx=10, pady=10)
tk.Button(root, text="Browse", command=browse_destination).grid(row=1, column=2, padx=10, pady=10)

# Copy button
tk.Button(root, text="Copy Files", command=copy_files).grid(row=2, columnspan=3, pady=20)

# Run the application
root.mainloop()
