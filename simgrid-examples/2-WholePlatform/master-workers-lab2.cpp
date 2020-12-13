/* Copyright (c) 2010-2019. The SimGrid Team. All rights reserved.          */

/* This program is free software; you can redistribute it and/or modify it
 * under the terms of the license (GNU LGPL) which comes with this package. */

/* ********************************************************************************** */
/* Take this tutorial online: https://simgrid.org/doc/latest/Tutorial_Algorithms.html */
/* ********************************************************************************** */

#include <simgrid/s4u.hpp>

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_masterworker, "Messages specific for this example");

static void worker(std::string id)
{
  std::string mailbox_name       = std::string("worker-") + id;
  simgrid::s4u::Mailbox* mailbox = simgrid::s4u::Mailbox::by_name(mailbox_name);
  
double compute_cost;
  do {
    double* msg  = static_cast<double*>(mailbox->get());
    compute_cost = *msg;
    delete msg;

    if (compute_cost > 0) /* If compute_cost is valid, execute a computation of that cost */
      simgrid::s4u::this_actor::execute(compute_cost);

  } while (compute_cost > 0); /* Stop when receiving an invalid compute_cost */

  XBT_INFO("Exiting now.");
}

static void master(std::vector<std::string> args)
{
  xbt_assert(args.size() == 4, "The master function expects 4 arguments");

  long tasks_count           = std::stol(args[1]);
  double compute_cost        = std::stod(args[2]);
  double communication_cost  = std::stod(args[3]);

  /* Get simulation engine instance */
  simgrid::s4u::Engine* e = simgrid::s4u::Engine::get_instance();
  std::vector<simgrid::s4u::Host*> hosts = e->get_all_hosts();
  long workers_count = hosts.size();

  XBT_INFO("Got %ld workers and %ld tasks to process", workers_count, tasks_count);

  /* Create workers */
  for (long i = 0; i < workers_count; i++) {
    simgrid::s4u::Actor::create(std::string("worker-") + std::to_string(i),
                                hosts[i],
				&worker,
                                std::to_string(i));
  }

  for (long i = 0; i < tasks_count; i++) { /* For each task to be executed: */
    /* - Select a worker in a round-robin way */
    std::string worker_rank        = std::to_string(i % workers_count);
    std::string mailbox_name       = std::string("worker-") + worker_rank;
    simgrid::s4u::Mailbox* mailbox = simgrid::s4u::Mailbox::by_name(mailbox_name);

    /* - Send the computation cost to that worker */
    XBT_INFO("Sending task %d of %ld to mailbox '%s'", i, tasks_count, mailbox->get_cname());
    mailbox->put(new double(compute_cost), communication_cost);
  }

  XBT_INFO("All tasks have been dispatched. Request all workers to stop.");
  for (long i = 0; i < workers_count; i++) {
    /* The workers stop when receiving a negative compute_cost */
    std::string worker_rank        = std::to_string(i % workers_count);
    std::string mailbox_name       = std::string("worker-") + worker_rank;
    simgrid::s4u::Mailbox* mailbox = simgrid::s4u::Mailbox::by_name(mailbox_name);

    mailbox->put(new double(-1.0), 0);
  }
}

int main(int argc, char* argv[])
{
  simgrid::s4u::Engine e(&argc, argv);
  xbt_assert(argc > 2, "Usage: %s platform_file deployment_file\n", argv[0]);

  /* Register the functions representing the actors */
  e.register_function("master", &master);
  // e.register_function("worker", &worker);

  /* Load the platform description and then deploy the application */
  e.load_platform(argv[1]);
  e.load_deployment(argv[2]);

  /* Run the simulation */
  e.run();

  XBT_INFO("Simulation is over");

  return 0;
}
