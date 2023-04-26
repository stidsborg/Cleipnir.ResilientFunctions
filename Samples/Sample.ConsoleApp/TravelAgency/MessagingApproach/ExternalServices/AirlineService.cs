using System.Threading.Tasks;

namespace ConsoleApp.TravelAgency.MessagingApproach.ExternalServices;

public static class AirlineService
{
    public static void Start()
    {
        MessageBroker.Subscribe(msg =>
        {
            if (msg is BookFlight booking)
            {
                Task.Run(async () =>
                {
                    await Task.Delay(250);
                    await MessageBroker.Send(new FlightBooked(booking.BookingId));
                });
            }

            return Task.CompletedTask;
        }); 
    }
}