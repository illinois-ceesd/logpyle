// expect to be called from python
async function chartsOutputGraph(id, x, ys, colors) {
	id = JSON.parse(id);
	x = JSON.parse(x);
	ys = JSON.parse(ys);

	// rebuild canvas before drawing to it
	let canvas = document.getElementById("chart" + id);
	let canvas_parent = canvas.parentElement;
	let new_canvas = document.createElement("canvas")
	new_canvas.id = "chart" + id

	canvas.remove();
	canvas_parent.appendChild(new_canvas);

	canvas.style.width='100%';
	canvas.style.height='100%';
	canvas.width  = canvas.offsetWidth;
	canvas.height = canvas.offsetHeight;

	let datasets = [];
	// add ys to dataset
	for (const [key, value] of Object.entries(ys)) {
		dataPairs = [];
		Object.entries(value["vals"]).forEach( (ele , index) => {
			dataPairs.push({x: x[index], y: ele[1]})
		} )

		datasets.push({
			data: dataPairs,
			label: key + " (" + value["units"] + ")",
			borderColor: value["color"],
		});
	}

	// create chart pointing to the file's chart canvas
	new Chart(document.getElementById("chart"+id), {
		type: 'scatter',
		data: {
			datasets: datasets
		},

		options: {
			scales: {
				type: 'linear',
				position: 'bottom'
			},
		},

	});
}


async function download(filename, contents) {
  var element = document.createElement('a');
  element.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(contents));
  element.setAttribute('download', filename);

  element.style.display = 'none';
  document.body.appendChild(element);

  element.click();

  document.body.removeChild(element);
}
