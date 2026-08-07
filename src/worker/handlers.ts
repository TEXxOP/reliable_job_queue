import { Job, JobHandler } from '../queue/types';

// this helps ensure each handler demonstrates idempotency patterns
// in production, you'd check a database flag before doing side effects

export const emailHandler: JobHandler = async (job: Job): Promise<void> => {
  const { to, subject, body } = job.payload as { to: string; subject: string; body: string };

  // this helps ensure we simulate real work with a random delay
  const processingTime = 500 + Math.random() * 2000;
  await new Promise((resolve) => setTimeout(resolve, processingTime));

  // this helps ensure we simulate occasional failures for testing retry logic
  if (Math.random() < 0.1) {
    throw new Error('SMTP connection timeout');
  }

  console.log(`Email sent to ${to}: "${subject}"`);
};

export const paymentHandler: JobHandler = async (job: Job): Promise<void> => {
  const { orderId, amount, currency } = job.payload as { orderId: string; amount: number; currency: string };

  const processingTime = 1000 + Math.random() * 3000;
  await new Promise((resolve) => setTimeout(resolve, processingTime));

  // this helps ensure we test the DLQ path with a small failure rate
  if (Math.random() < 0.05) {
    throw new Error('Payment gateway unavailable');
  }

  console.log(`Payment of ${amount} ${currency} processed for order ${orderId}`);
};

export const reportHandler: JobHandler = async (job: Job): Promise<void> => {
  const { reportType, userId } = job.payload as { reportType: string; userId: string };

  // this helps ensure report generation simulates a longer-running task
  const processingTime = 2000 + Math.random() * 5000;
  await new Promise((resolve) => setTimeout(resolve, processingTime));

  if (Math.random() < 0.08) {
    throw new Error('Report data source unavailable');
  }

  console.log(`Report "${reportType}" generated for user ${userId}`);
};

// this helps ensure all handlers are registered in one place
export const defaultHandlers: Map<string, JobHandler> = new Map([
  ['email', emailHandler],
  ['payment', paymentHandler],
  ['report', reportHandler],
]);
