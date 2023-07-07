from jinja2 import Environment, FileSystemLoader, BaseLoader, Template

enviroment = Environment(loader=FileSystemLoader("templates/"))
template = enviroment.get_template("index.html")

filename = "temp.html"
newFileHTML = open("templates/newFile.html", "r").read()
mainPy = open("main.py", "r").read()
mainPy = Environment(loader=BaseLoader).from_string(mainPy)
mainPy = mainPy.render(newFileHTML=newFileHTML)
mainCSS = open("main.css", "r").read()
runalyzerFile = open("modifiedRunalyzer.py", "r").read()

content = template.render(
        cssFile=mainCSS,
        pythonFile=mainPy,
        runalyzerFile=runalyzerFile,
        )
with open(filename, mode="w", encoding='utf-8') as message:
    message.write(content)
