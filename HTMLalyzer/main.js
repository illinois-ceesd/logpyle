// expect to be called from python
async function chartsOutputGraph(id, x, ys, colors) {
	id = JSON.parse(id);
	x = JSON.parse(x);
	ys = JSON.parse(ys);

	// resize canvas before drawing to it
	let canvas = document.getElementById("chart" + id);
	canvas.width = "300px"
	canvas.height = "150px"

	canvas.style.width='100%';
	canvas.style.height='100%';
	canvas.width  = canvas.offsetWidth;
	canvas.height = canvas.offsetHeight;

	let datasets = [];
	// add ys to dataset
	for (const [key, value] of Object.entries(ys)) {
		datasets.push({
			data: value["vals"],
			label: key + " (" + value["units"] + ")",
			borderColor: value["color"],
		});
	}

	// create chart pointing to the file's chart canvas
	new Chart(document.getElementById("chart"+id), {
		type: 'line',
		data: {
			labels: x,
			datasets: datasets
		},

		options: {
			responsive: true,
			plugins: {
				legend: {
					position: 'top',
				},
				title: {
					display: true,
					text: 'Chart.js Line Chart'
				}
			}
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



