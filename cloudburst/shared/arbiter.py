import ast
import inspect
import textwrap
import logging
import time
import cloudburst.shared.ast_analyzer as ast_analyzer
from cloudburst.shared.ast_analyzer import ROLLBACK_IDENTIFIER

DEPENDENT_ACCESS_THRESHOLD = 3
COMPARE_EXEC_COUNT = 10
FEEDBACK_EXEC_COUNT = 1000
ANNA_CLIENT_NAME = 'anna'
SHREDDER_CLIENT_NAME = 'shredder'

EXPECTATION_UPPER_BOUND = 2
EXPECTATION_FAIL_THRESHOLD = 0.1
# Evaluation results from profiling, only consider cache case for anna
def calc_anna_expectation(dependent_access_times):
    return 0.6 + dependent_access_times * 0.2

def calc_shredder_expectation():
    return 1

class Arbiter:
    ## Assume only one function pinned to executor
    def __init__(self):
        # Func meta
        self.func = None
        self.func_name = None
        self.func_ast = None
        self.func_args = None
        self.RPN_str = None
        # Profile
        self.exec_start_time = time.time()  ## There's no parallel execution
        self.expect_fail_count = 0
        self.feedback_exec_times = 0
        self.expectation = None
        # Rollback case
        self.rollback_flag = False
        self.compare_latencies = { ANNA_CLIENT_NAME: [], SHREDDER_CLIENT_NAME: [] }
        self.compare_decision = None
    
    def clear_compare(self):
        return NotImplementedError

    def clear_feedback(self):
        self.feedback_exec_times = 0
        self.expect_fail_count = 0

    def bind_func(self, func, func_name):
        logging.info(f'binding func {func_name}')
        self.func = func
        self.func_name = func_name
        self.func_ast = ast.parse(textwrap.dedent(inspect.getsource(func)))
        self.func_args = ast_analyzer.get_func_args(self.func_ast)
        ast_analyzer.get_funcdef_name(self.func_ast)
        self.RPN_str = ast_analyzer.generate_RPN_str(self.func_ast, self.func_args)
        if ROLLBACK_IDENTIFIER in self.RPN_str:
            # Cannot analyze args, rollback: run both sides then compare
            self.rollback_flag = True
        logging.info(f'ARG: {self.func_args}, RPN_str: {self.RPN_str}')
        
    def current_compare_client(self):
        if len(self.compare_latencies[ANNA_CLIENT_NAME]) < COMPARE_EXEC_COUNT:
            return ANNA_CLIENT_NAME
        elif len(self.compare_latencies[SHREDDER_CLIENT_NAME]) < COMPARE_EXEC_COUNT:
            return SHREDDER_CLIENT_NAME
        else:
            return None
        
    def exec_start(self):
        self.exec_start_time = time.time()
    
    def exec_end(self):
        elapsed = time.time() - self.exec_start_time
        self.feedback(elapsed)
        return elapsed
    
    # According to args, choose client & calc expectation
    def process_args(self, args):
        client_name = args[-1]
        if not isinstance(client_name, str) or client_name != 'arbiter':
            logging.info(f'Arbiter returns original args')
            return args
        
        assert len(args) == len(self.func_args) + 1, f'Final_arg len: {len(args)}, Func_arg len: {len(self.func_args)}'
        
        if self.rollback_flag:
            # Rollback case
            client_arg, expectation = self.rollback_compare()
        else:
            # Normal case
            arg_map = {}
            for arg_i, arg in enumerate(self.func_args):
                arg_map[arg] = args[arg_i + 1]
            
            dependent_access_times = ast_analyzer.calc(self.RPN_str, arg_map)
            if dependent_access_times > DEPENDENT_ACCESS_THRESHOLD:
                client_arg = SHREDDER_CLIENT_NAME
                expectation = calc_shredder_expectation()
            else:
                client_arg = ANNA_CLIENT_NAME
                expectation = calc_anna_expectation(dependent_access_times)
            logging.info(f'Dependent access: {dependent_access_times}')

        # Choose the better client
        final_args = args[:-1]
        final_args += (client_arg, )
        
        # Expectation
        self.expectation = expectation
        
        logging.info(f'Client choose: {client_arg}, Expectation: {self.expectation}')
        
        return final_args
    
    def rollback_compare(self):
        if self.compare_decision:
            return self.compare_decision, self.expectation
        
        cur_client = self.current_compare_client()
        if cur_client:
            return cur_client, None
        else:
            # We collected enough latencies for comparison
            self.compare_decision, expectation = self.compare_choose_client()
            logging.info(f'Rollback made decision: {self.compare_decision}, expectation: {expectation}')
            return self.compare_decision, expectation
    
    def compare_choose_client(self):
        # Choose lower median latency
        anna_median = self.compare_latencies[ANNA_CLIENT_NAME][len(self.compare_latencies[ANNA_CLIENT_NAME]) // 2]
        shredder_median = self.compare_latencies[SHREDDER_CLIENT_NAME][len(self.compare_latencies[SHREDDER_CLIENT_NAME]) // 2]
        if anna_median < shredder_median:
            return ANNA_CLIENT_NAME, anna_median
        else:
            return SHREDDER_CLIENT_NAME, shredder_median
    
    def feedback(self, elapsed):
        if self.rollback_flag and not self.compare_decision:
            # Rollback comparing
            self.compare_latencies[self.current_compare_client()].append(elapsed)
            return
        
        # Normal case, we already has expectation for every execution
        self.feedback_exec_times += 1
        if elapsed > EXPECTATION_UPPER_BOUND * self.expectation:
            self.expect_fail_count += 1
        if self.feedback_exec_times >= FEEDBACK_EXEC_COUNT:
            # Feedback verification
            if self.expect_fail_count > EXPECTATION_FAIL_THRESHOLD * FEEDBACK_EXEC_COUNT:
                # Failed, recalculate expectation and compare? Should we calc all latencies?
                return NotImplementedError
            
            self.clear_feedback()

        return