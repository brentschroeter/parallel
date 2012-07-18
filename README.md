Parallel
========

The Parallel project allows for easy parallel processing on networked clients and on multi-core machines.

Overview
-------------

Every ParallelClient object launches a ParallelWorker on a different thread that connects to a provided list of addresses (which includes the client's own address) in order to listen for work. If more than one CPU core is available, more ParallelWorkers will be created. Every ParallelClient object has the ability to push jobs to connected workers.

Testing on Many Machines
----------------------------------

Make sure that all of the computers are networked and that all firewalls are configured correctly. **This could mean disabling the firewall completely.** Install ZeroMQ and fire up `./test_network.py` on each computer. Each computer now has a worker running and is awaiting commands. Type `push` at the prompt on any computer to begin pushing jobs. Each job sleeps for about a half of a second and then returns 1.

Using FullImgTask and LiteImgTask
-----------------------------------------------

The tasks module includes three classes created specifically for working with images: `FullImgTask`, `LiteImgTask`, and `ImgOp`. When you use `FullImgTask` all of the image data is included in the task, meaning that it is sent in the same message as the instructions for what to do with the image. `LiteImgTask`, on the other hand, holds only the path to the file in question (the file must be in a shared location for this to work). Thus, the entire image is not sent with it, resulting in a much smaller message.

In order to define operations that can be performed by both of the `-ImgTask` classes, there is a third, abstract class called `ImgOp`. All subclasses of `ImgOp` must have a method `eval_img(self, img_data)` which returns the raw data for the altered image. If any additional arguments are needed, they must be stored as variables at initialization.