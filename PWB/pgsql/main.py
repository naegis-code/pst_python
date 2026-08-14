import tkinter as tk
from tkinter import ttk

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Tkinter Navigation Example")
        
        # Fullscreen configuration
        self.attributes('-fullscreen', True)
        self.bind("<Escape>", self.exit_fullscreen)
        
        # Container to hold frames
        container = tk.Frame(self)
        container.pack(side="top", fill="both", expand=True)
        
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)
        
        self.frames = {}
        for F in (StartPage, PageOne):
            page_name = F.__name__
            frame = F(parent=container, controller=self)
            self.frames[page_name] = frame
            frame.grid(row=0, column=0, sticky="nsew")
        
        self.show_frame("StartPage")
    
    def show_frame(self, page_name):
        frame = self.frames[page_name]
        frame.tkraise()

    def exit_fullscreen(self, event=None):
        self.attributes('-fullscreen', False)

class StartPage(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        label = ttk.Label(self, text="This is the start page")
        label.pack(side="top", fill="x", pady=10)
        
        button1 = ttk.Button(self, text="Go to Page One",
                            command=lambda: controller.show_frame("PageOne"))
        button1.pack()

class PageOne(tk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent)
        self.controller = controller
        label = ttk.Label(self, text="This is page one")
        label.pack(side="top", fill="x", pady=10)
        
        button = ttk.Button(self, text="Go to the Start Page",
                            command=lambda: controller.show_frame("StartPage"))
        button.pack()

if __name__ == "__main__":
    app = App()
    app.mainloop()
