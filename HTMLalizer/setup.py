from jinja2 import Environment, FileSystemLoader, Template

enviroment = Environment(loader=FileSystemLoader("templates/"))
template = enviroment.get_template("index.html")

filename = "temp.html"
mainPy = open("main.py", "r").read()
mainCSS = open("main.css", "r").read()
runalyzerFile = open("modifiedRunalyzer.py", "r").read()

content = template.render(
        cssFile=mainCSS,
        pythonFile=mainPy,
        runalyzerFile=runalyzerFile,
        )
with open(filename, mode="w", encoding='utf-8') as message:
    message.write(content)
